import threading
from dataclasses import dataclass, field

from common import fruit_item


@dataclass
class ClosingState:
    total_records: int
    coordinator_id: int
    counts_by_sum_id: dict[int, int] = field(default_factory=dict)
    global_count: int = 0


@dataclass
class ClientState:
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    partials_by_fruit: dict[str, fruit_item.FruitItem] = field(default_factory=dict)
    processed_count: int = 0
    closing: ClosingState | None = None


class SumState:
    def __init__(self):
        self._states_by_client: dict[str, ClientState] = {}
        self._states_lock = threading.Lock()

    def _get_client_state(self, client_id):
        with self._states_lock:
            return self._states_by_client.setdefault(client_id, ClientState())

    def add_data(self, client_id, fruit, amount):
        client_state = self._get_client_state(client_id)
        with client_state.lock:
            new_item = fruit_item.FruitItem(fruit, int(amount))
            current_item = client_state.partials_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0))
            client_state.partials_by_fruit[fruit] = current_item + new_item
            client_state.processed_count += 1

            if client_state.closing is not None:
                return client_state.processed_count, client_state.closing.coordinator_id

            return None

    def start_closing(self, client_id, total_records, coordinator_id):
        client_state = self._get_client_state(client_id)
        with client_state.lock:
            if client_state.closing is not None:
                return client_state.processed_count
            client_state.closing = ClosingState(total_records=total_records, coordinator_id=coordinator_id)
            return client_state.processed_count

    def add_worker_count(self, client_id, sender_sum_id, processed_count):
        client_state = self._get_client_state(client_id)
        with client_state.lock:
            if client_state.closing is None:
                return None
            
            total_records = client_state.closing.total_records
            if total_records == 0:
                return total_records

            previous_count = client_state.closing.counts_by_sum_id.get(sender_sum_id, 0)
            if processed_count <= previous_count:
                return None

            client_state.closing.counts_by_sum_id[sender_sum_id] = processed_count
            client_state.closing.global_count += processed_count - previous_count

            if client_state.closing.global_count == total_records:
                return total_records

            return None

    def take_flush_items(self, client_id):
        client_state = self._get_client_state(client_id)
        with client_state.lock:
            fruit_items = list(client_state.partials_by_fruit.values())
            client_state.partials_by_fruit.clear()

        with self._states_lock:
            self._states_by_client.pop(client_id, None)

        return fruit_items
