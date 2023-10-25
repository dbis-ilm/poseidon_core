### Poseidon Python API

In addition, there is a thin wrapper library for providing a Python API. This API allows creating and opening graph databases, as well as creating and accessing nodes and relationships.

In the following Python example the database `imdb` is created.

```python
import poseidon

# create a new graph pool and a graph database
pool = poseidon.create_pool("my_pool")
graph = pool.create_graph("imdb")
```

Next, we create a few nodes and relationships that are stored in the database.

```python
graph.begin()
a1 = graph.create_node("Actor", { "name": "John David Washington"})
a2 = graph.create_node("Actor", { "name": "Elizabeth Debicki"})
m1 = graph.create_node("Movie", { "title": "Tenet"})
graph.create_relationship(a1, m1, ":plays_in", {})
graph.create_relationship(a2, m1, ":plays_in", {})
graph.commit()
```

Nodes and relationships can be retrieved by their id. In addition, for a given node all its relationships can be obtained.

```python
graph.begin()
n = graph.get_node(m1)
for r in g.get_to_relationships(n.id):
    print(r, g.get_node(r.from_node), "-->", g.get_node(r.to_node))
```