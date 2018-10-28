import os

debug_level = os.environ.get('MONA_DEBUG')
if debug_level:
    import logging

    logging.basicConfig(
        style='{',
        format='[{asctime}.{msecs:03.0f}] {levelname}:{name}: {message}',
        datefmt='%H:%M:%S',
    )
    logging.getLogger('mona').setLevel(int(debug_level))
