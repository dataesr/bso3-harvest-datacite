def check_conformity(values_args: dict, class_table: type):
    values_k_v_args: list = list(values_args.items())
    harvest_state_table_attributes: dict = class_table.__annotations__

    for (key, value) in values_k_v_args:
        if key not in harvest_state_table_attributes:
            raise Exception(f"Element not available in '{class_table.__name__}' : '{key}'")
        type_element_to_update_arg: type = type(value)
        type_element_to_update_expected: type = harvest_state_table_attributes[key]

        if type_element_to_update_arg != type_element_to_update_expected:
            raise Exception(f"Element type not correct for '{class_table.__name__}' : got '{type_element_to_update_arg}' and expected '{type_element_to_update_expected}' for '{key}' attribute")


def check_conformity_and_get_where_clauses(where_args: dict, class_table: type) -> list:
    check_conformity(values_args=where_args, class_table=class_table)

    where_clauses: list = [class_table.__table__.c[key] == value for (key, value) in where_args.items()]

    return where_clauses
