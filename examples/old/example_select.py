import aerospike
from aerospike import predicates as p
from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import list_operations as lh

# Define host configuration
config = {
    'hosts': [('0.0.0.0', 3000)]
}
# Establishes a connection to the server
client = aerospike.client(config).connect()



# Create new write policy
write_policy = {'key': aerospike.POLICY_KEY_SEND}
update_policy = {'exists': aerospike.POLICY_EXISTS_UPDATE}




namespace = "test"
set_name = "test"


expr = exp.And(
    exp.LT(exp.IntBin("a"), 1000),
    exp.GT(exp.IntBin("a"), 1),
).compile()
scan_policy={"expressions": expr}


# Create the query object
query = client.query(namespace, set_name)
query.max_records = 1000

query.select("a","b","c","d")

# records = query.results(scan_policy)
records = query.results(scan_policy)
for r in records:
    print(r)
client.close()




