-- A connection carries no credential, not even the name of one: the workers on its queue hold
-- the forge tokens and choose one by the forge's host and the organisation (ADR 0014).
alter table connections drop column token_env;
