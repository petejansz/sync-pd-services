----    const ServiceStatus =
----    {
----        PREACTIVE: 1,
----        ACTIVE: 2,
----        SUSPEND: 3,
----        CLOSED: 4,
----        COMPLETED: 5
----    }
--
-- Update email verified:
update GMS4.sms_customer_contacts
    set status = 1, last_updated = current timestamp -4 days
    where contract_id in (select cc.contract_id from GMS4.sms_customer_contacts cc where cc.value like 'mansir-007%@mailinator.com' and cc.contact_type_id = 1)
;    
 -- Update PP service:
update GMS4.sms_customer_services
    set service_status_id = 2, last_updated = current timestamp -4 days
    where contract_id in (
        select cc.contract_id from GMS4.sms_customer_contacts cc 
            where cc.value like ( 'mansir-007%@mailinator.com' ) 
                and cc.contact_type_id = 1
            )
    AND service_type_id = 1
;
 -- Update SC service:
update GMS4.sms_customer_services
    set service_status_id = 3, last_updated = current timestamp -4 days
    where contract_id in (
        select cc.contract_id from GMS4.sms_customer_contacts cc 
            where cc.value like ( 'mansir-007%@mailinator.com' ) 
                and cc.contact_type_id = 1
            )
    AND service_type_id = 500
;
commit;
