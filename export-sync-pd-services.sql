--!!! UNCOMMENT THE EXPORT STATEMENT !!!
EXPORT TO sync-pd-services.csv OF DEL

select
    CONTRACT_IDENTITY,
    ACCOUNT_EMAIL,
    CONTRACT_ID,
    EMAIL_VERIFIED,
    listagg(cast(service_type_id as varchar(3)), ', ')   as SERVICE_TYPE_IDS,
    listagg(cast(service_status_id as varchar(3)), ', ') as SERVICE_STATUS_IDS
from (
    select
    c.contract_identity as CONTRACT_IDENTITY,
    cc.value AS ACCOUNT_EMAIL,
    cs.contract_id as CONTRACT_ID,
    cc.status  as EMAIL_VERIFIED,
    cast(cs.service_type_id as varchar(3)) as SERVICE_TYPE_ID,
    cast(cs.service_status_id as varchar(3)) as SERVICE_STATUS_ID
    from GMS4.sms_customer_services cs
        inner join GMS4.sms_customer_contacts cc on cc.contract_id = cs.contract_id
        inner join GMS4.sms_contracts c on c.contract_id = cc.contract_id
        where
            c.contract_status_id != 6 and -- not hidden
            cc.contact_type_id = 1 and
            cs.service_type_id in ( 1, 500 )    -- PP or SC
            and date(cc.last_updated) between current date -1 day and current timestamp -1 hour
    group by c.contract_identity, cc.value, cs.contract_id, cc.status, cs.service_type_id, cs.service_status_id
    )
group by contract_identity, ACCOUNT_EMAIL, contract_id, email_verified
order by account_email
;
