select *
from {{ metrics.calculate(
    metric('song_plays'),
    grain='day',
    dimensions=[],
) }}