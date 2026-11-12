-- Tags: no-old-analyzer, no-parallel
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: creates a server-global settings profile under a fixed name.
-- Random settings limits: automatic_parallel_replicas_mode=(0, 0)

-- Restoring a remembered pre-adjustment value must be judged under the current settings
-- constraints, not just the ones active when the value was remembered: a profile switch
-- mid-session can forbid a value the derived plan is about to restore. Before this was fixed,
-- the restore below put automatic_parallel_replicas_mode back to 1, bypassing the profile's
-- MAX 0 that arrived after the value was remembered.
--
-- automatic_parallel_replicas_mode = 1 arrives through a profile, not a bare SET: clickhouse-client
-- mirrors every bare SET and resends it with each later query it sends, but it never mirrors a
-- profile switch's own settings. A bare SET here would get resent on the SET below that installs
-- the MAX 0 profile, and the server would reject that resend outright, before the derived plan's
-- own restore logic ever ran.

DROP SETTINGS PROFILE IF EXISTS profile_05631_value;
DROP SETTINGS PROFILE IF EXISTS profile_05631_const;

CREATE SETTINGS PROFILE profile_05631_value SETTINGS automatic_parallel_replicas_mode = 1;
SET profile = 'profile_05631_value';
SET distributed_plan_workers_num = 3;

CREATE SETTINGS PROFILE profile_05631_const SETTINGS automatic_parallel_replicas_mode MAX 0;
SET profile = 'profile_05631_const';

SET distributed_plan_workers_num = 0;

SELECT 'restoring under a constraint that now forbids the remembered value resets to default';
SELECT getSetting('automatic_parallel_replicas_mode');

DROP SETTINGS PROFILE profile_05631_value;
DROP SETTINGS PROFILE profile_05631_const;
