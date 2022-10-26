#pragma once

#include <unordered_map>
#include <mutex>

namespace local_engine
{
template <typename K, typename V>
class ConcurrentMap {
public:
    void insert(const K& key, const V& value) {
        std::lock_guard lock{mutex};
        map.insert({key, value});
    }

    V get(const K& key) {
        std::lock_guard lock{mutex};
        auto it = map.find(key);
        if (it == map.end()) {
            return nullptr;
        }
        return it->second;
    }

    void erase(const K& key) {
        std::lock_guard lock{mutex};
        map.erase(key);
    }

    void clear() {
        std::lock_guard lock{mutex};
        map.clear();
    }

    size_t size() const {
        std::lock_guard lock{mutex};
        return map.size();
    }

private:
    std::unordered_map<K, V> map;
    mutable std::mutex mutex;
};
}

