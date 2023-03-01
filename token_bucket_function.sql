DROP FUNCTION IF EXISTS take_token(varchar);
DROP FUNCTION IF EXISTS take_token_with_rate(varchar, int4, int4);

CREATE OR REPLACE FUNCTION take_token_with_rate (rate_key varchar(100), rate int4, rate_period int4) RETURNS boolean AS $$
DECLARE
    tokens int4;
    extra_tokens int4;
    new_tokens int4;
    last_refill timestamptz;
    this_refill timestamptz;
BEGIN

    -- Lock the buckets until end of transaction
    LOCK TABLE token_buckets IN EXCLUSIVE MODE;

    -- Read current tokens and last take
    SELECT b.tokens, b.last_refill INTO tokens, last_refill FROM token_buckets b WHERE b.key_id = rate_key;
    IF tokens IS NULL THEN
        tokens := rate; -- Start with the max amount of tokens
        last_refill = now();
        raise notice 'Setting up a bucket for key % with % tokens', $1, tokens;
        INSERT INTO token_buckets VALUES ($1, tokens, last_refill);
    END IF;

    -- Calculate newly generated tokens since last call
    extra_tokens := floor(
        EXTRACT(EPOCH FROM (now() - last_refill) * rate / rate_period)
    )::int4;
    this_refill := last_refill + (extra_tokens * interval '1 second' * rate_period / rate);
    new_tokens := LEAST(rate, tokens + extra_tokens);
    raise notice 'Key % has % tokens, last batch generated at %', $1, new_tokens, this_refill;

    -- If there are no tokens left then we don't need to do anything
    IF new_tokens <= 0 THEN
        RETURN FALSE;
    END IF;

    -- Set new values and return
    UPDATE token_buckets b SET (tokens, last_refill) = (new_tokens - 1, this_refill) WHERE b.key_id = $1;
    RETURN TRUE;
END
$$ LANGUAGE plpgsql;    

-- Function
CREATE OR REPLACE FUNCTION take_token (rate_key varchar(100)) RETURNS boolean AS $$
DECLARE
    rate int4;
    rate_period int4;
BEGIN
    -- Check if this user exists
    SELECT INTO rate, rate_period r.per_period, r.period_seconds FROM token_rates r WHERE r.key_id = rate_key;
    IF rate IS NULL OR rate_period IS NULL THEN
        raise notice 'Key % does not have a rate configured', $1;
        RETURN FALSE;
    END IF;
    
    RETURN take_token_with_rate(rate_key, rate, rate_period);
END
$$ LANGUAGE plpgsql;
