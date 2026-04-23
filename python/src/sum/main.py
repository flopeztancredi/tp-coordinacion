import logging
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

    def start(self):
        control_thread = threading.Thread(
            target=self._control_plane.start,
            name=f"sum-{config.ID}-control-plane",
            daemon=True,
        )
        control_thread.start()
        self._control_plane.wait_until_ready()
        self._data_plane.start()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_service = SumService()
    sum_service.start()
    return 0


if __name__ == "__main__":
    main()
