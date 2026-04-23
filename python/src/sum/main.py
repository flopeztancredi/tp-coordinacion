import logging
import signal
import threading

import config
from coordination.control_plane import SumControlPlane
from coordination.data_plane import SumDataPlane
from coordination.state import SumState


class SumService:
    def __init__(self):
        self._state = SumState()
        self._control_plane = SumControlPlane(self._state)
        self._data_plane = SumDataPlane(self._state)
        self._control_thread = None

    def start(self):
        self._control_thread = threading.Thread(
            target=self._run_control_plane,
            name=f"sum-{config.ID}-control-plane",
            daemon=False,
        )
        self._control_thread.start()

        try:
            self._control_plane.wait_until_ready()
            self._run_data_plane()
        finally:
            self._control_plane.stop()
            self._control_thread.join(timeout=5)

    def stop(self):
        self._data_plane.stop()
        self._control_plane.stop()

    def close(self):
        self._data_plane.close()
        self._control_plane.close()

    def _run_data_plane(self):
        try:
            self._data_plane.start()
        except Exception:
            logging.exception("Data plane error")
        finally:
            self._data_plane.stop()

    def _run_control_plane(self):
        try:
            self._control_plane.start()
        except Exception:
            logging.exception("Control plane error")


def _create_sigterm_handler(sum_service):
    def handler(*_):
        logging.info("SIGTERM received, shutting down sum gracefully")
        sum_service.stop()
    return handler


def main():
    logging.basicConfig(level=logging.INFO)
    sum_service = SumService()
    signal.signal(signal.SIGTERM, _create_sigterm_handler(sum_service))

    try:
        sum_service.start()
    except Exception:
        logging.exception("Sum service error")
    finally:
        sum_service.close()
        logging.info("Sum service shut down")

    return 0


if __name__ == "__main__":
    main()
