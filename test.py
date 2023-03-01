from pg import DB

# Connect to Postgres
db = DB()

# Create user 'foo' wih rate 3600
db.query('DELETE FROM token_buckets WHERE key_id = \'foo\';')
db.query('INSERT INTO token_rates (key_id, per_period, period_seconds) VALUES (\'foo\', 3600, 3600) ON CONFLICT (key_id) DO UPDATE SET per_period = EXCLUDED.per_period, period_seconds = EXCLUDED.period_seconds;')

# Disable "NOTICE" messages
db.query('SET client_min_messages TO WARNING;')

# Try to get 5000 tokens
token_counter = 0
for i in range(5000):
    q = db.query("SELECT take_token('foo')")
    if q.getresult()[0][0]:
        token_counter += 1

print("Got {} tokens in total".format(token_counter))

