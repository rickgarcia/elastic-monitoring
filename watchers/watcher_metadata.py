
# ES Watcher metadata settings for filesystem_usage_*
fs_metadata = {
    'example': {
        'cluster_name': 'example',
        'filesystem': '/opt/elastic',
        'name': 'Watcher Description',
        'threshold': 0.9,
        'window_period': '5m'
    }
}

metadata = {
    'filesystem_usage': fs_metadata
}

if (__name__ == 'main'):
    pass
