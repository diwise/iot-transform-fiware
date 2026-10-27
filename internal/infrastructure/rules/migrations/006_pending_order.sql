-- New producers enqueue complete relation sets under the tenant mutation
-- lock. DrainOnce delivers in this order per (tenant, entity_id), holding
-- the row lock through HTTP + acknowledgement. Existing pending rows must
-- be drained before upgrading: their historical commit order is unknown.
ALTER TABLE pending_broker_work ADD COLUMN IF NOT EXISTS delivery_order BIGSERIAL;
CREATE UNIQUE INDEX IF NOT EXISTS pending_broker_work_order ON pending_broker_work(delivery_order);
CREATE INDEX IF NOT EXISTS pending_broker_work_entity_order ON pending_broker_work(tenant, entity_id, delivery_order);
