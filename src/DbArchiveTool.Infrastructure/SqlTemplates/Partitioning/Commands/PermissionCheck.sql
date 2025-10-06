SELECT permission_name
FROM fn_my_permissions({ObjectName}, 'OBJECT')
WHERE permission_name IN ('ALTER', 'CONTROL', 'VIEW DEFINITION');
