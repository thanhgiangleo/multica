-- Attach the CONCURRENTLY-built unique index as the table's primary key.
ALTER TABLE project_member
    ADD CONSTRAINT project_member_pkey PRIMARY KEY USING INDEX project_member_pkey_uidx;
