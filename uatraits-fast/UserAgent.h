#pragma once

#include <contrib/libs/clickhouse/libs/libcommon/include/ext/singleton.h>
#include <metrika/core/libs/appmetrica/types/Browser.h>
#include <metrika/core/libs/appmetrica/types/BrowserBase.h>
#include <metrika/core/libs/appmetrica/types/OperatingSystem.h>
#include <metrika/core/libs/statdaemons/mobile/server/Request.h>
#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <chrono>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>

namespace components
{

class UserAgent : public ext::singleton<UserAgent>
{
friend class ext::singleton<UserAgent>;

public:
    void create(const Poco::Util::AbstractConfiguration & config);

    bool isReady() const;
    void reload();
    const std::chrono::seconds & getReloadFrequency() const;

    class OperatingSystem
    {
    friend class UserAgent;
    public:
        appmetrica::types::OperatingSystem getName() const;
        const UATraits::Version & getVersion() const;

    private:
        OperatingSystem(const std::string & os, const UATraits::Version & version);

        const appmetrica::types::OperatingSystem operating_system;
        const UATraits::Version version;
    };

    class Browser
    {
    friend class UserAgent;
    public:
        Browser(const appmetrica::types::Browser browser, const UATraits::Version & version);

        const std::optional<appmetrica::types::Browser> & getName() const;
        const std::optional<appmetrica::types::BrowserBase> & getBase() const;
        const UATraits::Version & getVersion() const;
        const UATraits::Version & getBaseVersion() const;

    private:
        Browser(const std::string & name, const UATraits::Version & version, const std::string & browser_base, const UATraits::Version & version_base);

        const std::optional<appmetrica::types::Browser> browser;
        const std::optional<appmetrica::types::BrowserBase> browser_base;

        const UATraits::Version version;
        const UATraits::Version version_base;
    };

    class Agent
    {
    friend class UserAgent;
    public:
        bool hasSameSiteSupport() const;
        bool isBrowser() const;
        bool isItpEnabled() const;
        bool isMobile() const;
        const OperatingSystem & getOperatingSystem() const;
        const Browser & getBrowser() const;

    private:
        Agent(
            const bool has_same_site_support,
            const bool is_mobile,
            const bool is_browser,
            const bool is_itp_enabled,
            const OperatingSystem & os,
            const Browser & browser);

        const bool has_same_site_support;
        const bool is_browser;
        const bool is_itp_enabled;
        const bool is_mobile;
        const OperatingSystem operating_system;
        const Browser browser;
    };

    Agent detect(const server::Request & request) const;
    Agent detect(const std::string & user_agent) const;

private:
    UserAgent() = default;

    Agent detect(
        const std::string & user_agent,
        const std::string & profile,
        const std::string & opera_mini_user_agent) const;

    std::chrono::seconds reload_frequency;
    std::string browsers_path;
    std::string profiles_path;
    std::string extra_path;

    std::unique_ptr<UATraits> ua_traits;
    mutable std::shared_mutex ua_traits_mutex;
};

} // namespace components
