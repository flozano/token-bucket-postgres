from pg import DB
import time
# Connect to Postgres
db = DB()

# Create user 'foo' wih rate 3600

db.query('DELETE FROM token_buckets WHERE key_id = \'foo\';')
db.query('INSERT INTO token_rates (key_id, per_period, period_seconds) VALUES (\'foo\', 3600, 3600) ON CONFLICT (key_id) DO UPDATE SET per_period = EXCLUDED.per_period, period_seconds = EXCLUDED.period_seconds;')

# Disable "NOTICE" messages
db.query('SET client_min_messages TO WARNING;')

# Try to get 5000 tokens
print("try 5000, take 3600")
token_counter = 0
for i in range(5000):
    q = db.query("SELECT take_token('foo')")
    if q.getresult()[0][0]:
        token_counter += 1

print("Got {} tokens in total".format(token_counter))


db.query('DELETE FROM token_buckets WHERE key_id = \'k1\';')
print("try 20, take 10 in 5 seconds, wait 5 seconds and try again 20")
for r in range(2):
    c = 0
    for i in range(20):
        q = db.query('SELECT take_token_with_rate(\'k1\', 10, 5)')
        if q.getresult()[0][0]:
            c += 1
    print("Got {} tokens (round {})".format(c, r))
    time.sleep(5)
