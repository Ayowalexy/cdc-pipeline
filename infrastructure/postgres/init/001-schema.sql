-- Runs once, on first initialisation of the postgres data volume.
CREATE TABLE IF NOT EXISTS public.orders (
  id            TEXT PRIMARY KEY,
  status        TEXT          NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'PROCESSING', 'COMPLETED', 'CANCELLED')),
  total_amount  NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
  currency      CHAR(3),
  version       INTEGER       NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Delete events must carry the full row (including version) so the consumer can write a
-- versioned tombstone. With the default identity Debezium's `before` would only hold the PK.
ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- Publication used by the Debezium pgoutput plugin (publication.autocreate.mode=disabled).
CREATE PUBLICATION orders_publication FOR TABLE public.orders;
