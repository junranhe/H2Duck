package hdserver;

public enum CommandSqlType {
    other,
    create_table,
    execute,
    rollback,
    commit,
    create_index,
    select,
    update,
    insert,
    delete,
    set,
    use,
    create_function,
    create_procedure,
    create_view,
    alter,
    drop,
    begin
}
