{{ config(severity='warn') }}

-- Portal add-on: data-quality signal, not a gate. Requests whose closed_date precedes their
-- created_date. FY2026 has exactly one (SR26-00411461, an E-Scooter Parking Complaint closed
-- 33 minutes "before" it was created, same day). It stays in the data; its hours_to_close is
-- negative, so it counts as met. Warns so a growing number is visible rather than silent.
select sr_number, sr_type, created_date, closed_date
from {{ ref('stg_portal_requests') }}
where closed_date < created_date
