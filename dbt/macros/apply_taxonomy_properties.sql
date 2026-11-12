{#
  Stamp every table dbt builds with the taxonomy properties required by
  governance/taxonomy.yml (data_owner, domain, layer, description, pii).
  Values come from the model's YAML: description + meta.
#}
{% macro apply_taxonomy_properties() %}
  {%- set meta = model.config.get('meta', {}) -%}
  {%- set props = {
      'data_owner': meta.get('data_owner', 'revops-data@company.example'),
      'domain': meta.get('domain', 'revenue'),
      'layer': meta.get('layer', 'silver'),
      'description': (model.description or 'TODO: describe this model') | replace("'", "''"),
      'pii': meta.get('pii', 'none'),
  } -%}
  alter table {{ this }} set properties extra_properties = MAP(
    ARRAY[{% for k in props %}'{{ k }}'{{ ',' if not loop.last }}{% endfor %}],
    ARRAY[{% for v in props.values() %}'{{ v }}'{{ ',' if not loop.last }}{% endfor %}]
  )
{% endmacro %}
