{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'hash'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_test_normalization_namespace",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('simple_stream_with_n__lting_into_long_names_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        adapter.quote('date'),
    ]) }} as _airbyte_simple_stre__nto_long_names_hashid,
    tmp.*
from {{ ref('simple_stream_with_n__lting_into_long_names_ab2') }} tmp
-- simple_stream_with_n__lting_into_long_names
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

