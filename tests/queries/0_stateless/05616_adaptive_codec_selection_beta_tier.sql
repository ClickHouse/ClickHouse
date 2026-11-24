SELECT name, tier FROM system.merge_tree_settings
WHERE name IN ('enable_adaptive_codec_selection', 'adaptive_codec_selection_period')
ORDER BY name;
