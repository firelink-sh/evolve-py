from evolve.io import Bytes
from evolve.ir import BytesBackend


def test_bytes_local_file():
    source = Bytes("examples/data/dummy.csv", backend=BytesBackend())
    ir = source.read()

    assert len(ir) == 120
