-- Portal add-on: landed count = portal count. A partition marked 'verified' must have landed
-- exactly the rows the portal reported both before and after paging it.
select pull_id, partition_date, expected_count, expected_count_after, landed_count
from {{ source('chicago_portal', 'portal_pulls') }}
where status = 'verified'
  and (landed_count is null
       or landed_count <> expected_count
       or expected_count <> expected_count_after)
