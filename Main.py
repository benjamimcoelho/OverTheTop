import argparse
import logging
import re
import threading

from OverTheTop import OTT, defaults
from UI.UI import UI


def setup_root_logger(debug=False):
    logging.basicConfig(format="[%(levelname)-5s - %(asctime)s] %(message)s",  # %(threadName)s
                        level=logging.INFO if not debug else logging.DEBUG, datefmt="%H:%M:%S")


# parse program arguments

def parse_args():
    parser = argparse.ArgumentParser(description="Over The Top Streaming Service")

    parser.add_argument("neighbours", nargs='*',
                        help="The neighbours for which the service will connect (Format : IPAddress:Port). The "
                             "neighbours can also be connected at a later date with the service's UI")
    parser.add_argument("-p", "--port", type=int, default=None,
                        help=f"The port for which the service will be bound (default={defaults.DEFAULT_PORT})")
    parser.add_argument("-a", "--address", type=str, default=None,
                        help="The address for which the service will be bound (default= default_hostname_ip)")
    parser.add_argument("-n", "--name", type=str, default=None,
                        help="The name given the node (optional)")
    parser.add_argument("-dbg", "--debug", action='store_true',
                        help="Runs the service in debug mode. Effect: Logger will also print debug messages")
    parser.add_argument("-cli", "--client", action='store_true',
                        help=("Runs the service in a makeshift client mode that attempts to connect to one of the "
                              "given addresses. Connection requests won't be accepted but the service can still "
                              "route flow if connected manualy to other nodes through the UI."))
    parser.add_argument("-src", "--source", nargs='*', type=str, default=None,
                        help="The sources of the instance yielded flows. The sources can be given at a later date "
                             "with the service's UI")
    parser.add_argument("-nogui", "--nogui", action='store_true',
                        help="Disables UI. As a result flow players can no longer be called.")

    return parser.parse_args()


def wait_quit():
    while True:
        if re.search(f"(?i:quit)|(?i:exit)", input()):
            break


if __name__ == '__main__':
    exit_code = 0
    main = None
    args = parse_args()
    setup_root_logger(args.debug)

    ott = OTT(bind_address=args.address, bind_port=args.port, name=args.name)

    if args.client:
        for arg in args.neighbours:
            try:
                add, port = arg.split(':')
                ott.connect((add, int(port)))
                ott.client()
                break
            except (TypeError, ValueError):
                logging.error(f"Invalid address format for argument '{arg}'")
            except ConnectionRefusedError:
                logging.warning(f"Connection refused by '{arg}'")
    else:
        for arg in args.neighbours:
            try:
                add, port = arg.split(':')
                ott.connect((add, int(port)))
            except (TypeError, ValueError):
                logging.error(f"Invalid address format for argument '{arg}'")
            except ConnectionRefusedError:
                logging.warning(f"Connection refused by '{arg}', ignoring...")
        main = threading.Thread(target=ott.run, name="OTT")

    if main:
        main.start()
    logging.debug("Engaging UI")

    if args.source:
        for s in args.source:
            ott.yield_flow(s)
    try:
        if not args.nogui:
            ui = UI(ott, args.name)
            try:
                ui.destroy()
            except Exception:
                logging.warning("Main cannot confirm UI's death")
        elif main is None:
            logging.critical("Client mode requires UI")
            exit_code = -2
        else:
            wait_quit()
    except KeyboardInterrupt:
        exit_code = -1
    except Exception:
        logging.exception("UI Exception")
    try:
        ott.stop()
    except Exception:
        logging.exception("WELP")
    if main:
        main.join()

    exit(exit_code)
