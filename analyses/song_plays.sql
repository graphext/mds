select *
from {{ metrics.calculate(
    metric('song_plays'),
    grain='day',
    dimensions=['level'],
) }}
order by date_day desc, level