import os

debug_level = os.environ.get('CAF_DEBUG')
if debug_level:
    import logging

    logging.basicConfig(
        style='{',
        format='[{asctime}.{msecs:03.0f}] {levelname}:{name}: {message}',
        datefmt='%H:%M:%S',
    )
    logging.getLogger('caf').setLevel(int(debug_level))
