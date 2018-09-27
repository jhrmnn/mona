import os

debug_level = os.environ.get('CAF_DEBUG')
if debug_level:
    import logging
    logging.basicConfig()
    logging.getLogger('caf2').setLevel(int(debug_level))
