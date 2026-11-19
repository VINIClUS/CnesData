# Key schema changes replace the table (blocked by prevent_destroy only when state exists) or
# rebuild indexes in place (never blocked by it), so the approved schema is pinned here.
locals {
  approved_index_names = [for position in range(1, 7) : "gsi${position}"]
  approved_global_secondary_indexes = {
    for name in local.approved_index_names : name => {
      hash_key        = "${name}pk"
      range_key       = "${name}sk"
      projection_type = "ALL"
    }
  }
  approved_key_attributes = concat(["pk", "sk"], flatten([
    for index in values(local.approved_global_secondary_indexes) : [index.hash_key, index.range_key]
  ]))
  approved_key_schema = {
    hash_key                 = "pk"
    range_key                = "sk"
    attributes               = sort([for name in local.approved_key_attributes : "${name}:S"])
    global_secondary_indexes = local.approved_global_secondary_indexes
  }
  # Planned index blocks are not read: any reference to them emits the provider deprecation
  # warning for index hash_key/range_key on every plan, so the declared index map is compared.
  planned_key_schema = {
    hash_key  = aws_dynamodb_table.control_plane.hash_key
    range_key = aws_dynamodb_table.control_plane.range_key
    attributes = sort([
      for attribute in aws_dynamodb_table.control_plane.attribute :
      "${attribute.name}:${attribute.type}"
    ])
    global_secondary_indexes = local.global_secondary_indexes
  }
}

resource "terraform_data" "key_schema_policy" {
  lifecycle {
    precondition {
      condition     = jsonencode(local.planned_key_schema) == jsonencode(local.approved_key_schema)
      error_message = "key_schema_changed planned=${jsonencode(local.planned_key_schema)}"
    }
  }
}
