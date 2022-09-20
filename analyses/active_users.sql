select *
from {{ metrics.calculate(
    metric('active_users'),
    grain='day',
    dimensions=['level'],
) }}
order by date_day desc, level