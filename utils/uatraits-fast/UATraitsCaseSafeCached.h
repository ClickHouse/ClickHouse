#pragma once

#include <dbms/src/Common/SipHash.h>

#include <contrib/libs/clickhouse/libs/libcommon/include/ext/unlock_guard.h>

#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>


/** Вариант UATraits с кэшом готовых результатов и сохранением регистра в выходных полях.
  * В качестве кэша используется cache table
  *  (open addressing хэш-таблица фиксированного размера
  *   без механизма разрешения коллизий, с автоматическим вытеснением)
  *  под mutex-ом.
  * В качестве хэш-функции используется SipHash128. Предполагается, что коллизии не имеют значения.
  *
  * Mutex блокируется на короткое время, которое не включает в себя тяжёлые вычисления,
  *  поэтому масштабирование на несколько потоков нормальное.
  */
class UATraitsCaseSafeCached : private boost::noncopyable
{
public:
    UATraitsCaseSafeCached(const std::string & browser_path_, const std::string & profiles_path_, const std::string & extra_path_)
        : table(std::make_unique<Cell []>(cache_size))
        , impl(browser_path_, profiles_path_, extra_path_)
    {
    }

    void load()
    {
        if (impl.load())
            table = std::make_unique<Cell []>(cache_size);
    }

    /** См. документацию к функции UATraits::detectCaseSafe().
      */
    void detectCaseSafe(
        StringRef user_agent,
        StringRef user_agent_lower,
        StringRef profile,
        StringRef x_operamini_phone_ua,
        StringRef x_operamini_phone_ua_lower,
        UATraits::Result & result,
        UATraits::MatchedSubstrings & matched_substrings) const
    {
        if (!user_agent.size && !profile.size && !x_operamini_phone_ua.size)
            return;

        if (profile.size || x_operamini_phone_ua.size)
        {
            impl.detectCaseSafe(user_agent, user_agent_lower, profile, x_operamini_phone_ua, x_operamini_phone_ua_lower, result, matched_substrings);
            return;
        }

        UInt128 hash;

        sipHash128(user_agent.data, user_agent.size, hash.ui128);

        size_t index = hash.lo % cache_size;

        auto & cell = table[index];

        std::lock_guard<std::mutex> lock(mutex);

        if (cell.first == hash)
        {
            ++tries;
            ++hits;
            result = cell.second;
        }
        else
        {
            ++tries;

            {
                ext::unlock_guard<std::mutex> unlock(mutex);
                impl.detectCaseSafe(user_agent, user_agent_lower, profile, x_operamini_phone_ua, x_operamini_phone_ua_lower, result, matched_substrings);
            }

            cell.first = hash;
            cell.second = result;
        }
    }

    double getHitRate() const
    {
        return static_cast<double>(hits) / tries;
    }

private:
    static const size_t cache_size = 8192;

    union UInt128
    {
        struct
        {
            UInt64 lo;
            UInt64 hi;
        };
        char ui128[16] = {};

        bool operator== (const UInt128 & rhs) const { return lo == rhs.lo && hi == rhs.hi; }
    };

    typedef std::pair<UInt128, UATraits::Result> Cell;

    std::unique_ptr<Cell []> table;

    mutable std::mutex mutex;

    UATraits impl;

    mutable size_t tries = 0;
    mutable size_t hits = 0;
};
