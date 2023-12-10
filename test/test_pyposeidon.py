import pytest
import os
import sys
sys.path.append('../build')

# import after sys.path is set!
import poseidon

path = "/mnt/pmem0/poseidon/py_tst"

# TODO: should contain more tests of the Python API

def setup_function():
    global path
    if os.path.isdir('/mnt/pmem0/poseidon'):
        path = "/mnt/pmem0/poseidon/py_tst"
    else:
        path = "py_test_pool"

def teardown_function():
    if os.path.isdir('/mnt/pmem0/poseidon'):
        try:
            os.remove(path)
        except:
            pass

def test_create_pool():
    p = poseidon.create_pool(path, 1024 * 1024 * 80)
    assert p != None
    g = p.create_graph("my_py_graph1", 1000)
    assert g != None
    p.drop_graph("my_py_graph1")
    p.close()


# def test_open_graph():
#    p = poseidon.create_pool(path, 1024 * 1024 * 80)
#    assert p != None
#    with pytest.raises(RuntimeError):
#        g = p.open_graph("my_py_graph2", 1000)
#    p.close()

def test_create_node():
    p = poseidon.create_pool(path, 1024 * 1024 * 80)
    assert p != None
    g = p.create_graph("my_py_graph3", 1000)
    assert g != None
    g.begin()
    a1 = g.create_node("Actor", { "name": "John David Washington"})
    m1 = g.create_node("Movie", { "title": "Tenet"})
    g.commit()
    g.begin()
    anode = g.get_node(a1)
    mnode = g.get_node(m1)
    g.abort()
    p.drop_graph("my_py_graph3")
    p.close()
