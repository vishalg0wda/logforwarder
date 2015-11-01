
import sys
import config
import logging
import optparse
from os import path
from runner import event_loop

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s'
                   )

def main():
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', action="store", dest="config",
                      help="path to *.json file")
    options, _ = parser.parse_args()
    config_file = options.config or 'config.json'
    if not path.exists(config_file):
        sys.exit('please specify a config file')
    config.assign_globals(config_file)
    event_loop()

if __name__ == '__main__':
    main()
