--
--1. Send data to multiple outputs
---multiple select statements INTO, referencing with clause ReaderQuery
WITH ReaderQuery AS (
	SELECT
		*
	FROM
		Input TIMESTAMP BY Time
)

SELECT * INTO ArchiveOutput FROM ReaderQuery --output 1

SELECT 
	Make,
	System.TimeStamp() AS Time,
	COUNT(*) AS [Count] 
INTO AlertOutput 
FROM ReaderQuery
GROUP BY
	Make,
	TumblingWindow(second, 10)
HAVING [Count] >= 3 --output 2


--2. Calculation over past events
---using LAG function. LAG(scalar, offset) OVER (partition by x limit duration(unit, length))
SELECT
	Make,
	Time
FROM
	Input TIMESTAMP BY Time
WHERE
	LAG(Make, 1) OVER (LIMIT DURATION(minute, 1)) <> Make


--3. Return last event over window
WITH LastInWindow AS
(
	SELECT 
		MAX(Time) AS LastEventTime
	FROM 
		Input TIMESTAMP BY Time
	GROUP BY 
		TumblingWindow(minute, 10)
)

SELECT 
	Input.License_plate,
	Input.Make,
	Input.Time
FROM
	Input TIMESTAMP BY Time 
	INNER JOIN LastInWindow
	ON DATEDIFF(minute, Input, LastInWindow) BETWEEN 0 AND 10
	AND Input.Time = LastInWindow.LastEventTime



--4.Data aggregation over time
SELECT
	Make,
	COUNT(*) AS Count
FROM
	Input TIMESTAMP BY Time
GROUP BY
	Make,
	TumblingWindow(second, 10)


--5. Periodically output values
---to handle sparse events and when we want continuous output
---use hopping window 
SELECT
	System.Timestamp() AS Window_end,
	TopOne() OVER (ORDER BY Time DESC) AS Last_event --Topone
FROM
	Input TIMESTAMP BY Time
GROUP BY
	HOPPINGWINDOW(second, 300, 5) --every 5 seconds return the last event in the past 5 mins window


--6. Correlate events in stream
--sample input
'''
| Make | License_plate | Time |
| --- | --- | --- |
| Make1 |ABC-123 |2023-01-01T00:00:01.0000000Z |
| Make1 |AAA-999 |2023-01-01T00:00:02.0000000Z |
| Make2 |DEF-987 |2023-01-01T00:00:03.0000000Z |
| Make1 |GHI-345 |2023-01-01T00:00:04.0000000Z |
'''

--sample output
'''
| Make | Time | Current_car_license_plate | First_car_license_plate | First_car_time |
| --- | --- | --- | --- | --- |
| Make1 |2023-01-01T00:00:02.0000000Z |AAA-999 |ABC-123 |2023-01-01T00:00:01.0000000Z |
'''
SELECT
	Make,
	Time,
	License_plate AS Current_car_license_plate,
	LAG(License_plate, 1) OVER (LIMIT DURATION(second, 90)) AS First_car_license_plate,
	LAG(Time, 1) OVER (LIMIT DURATION(second, 90)) AS First_car_time
FROM
	Input TIMESTAMP BY Time
WHERE
	LAG(Make, 1) OVER (LIMIT DURATION(second, 90)) = Make


--7. Detect duration between events

---LAST to retrieve last event with specific condition
SELECT
	[user],
	feature,
	DATEDIFF(
		second,
		LAST(Time) OVER (PARTITION BY [user], feature LIMIT DURATION(hour, 1) WHEN Event = 'start'),
		Time) as duration
FROM input TIMESTAMP BY Time
WHERE
	Event = 'end'



--8. Retrieve first event of a window
---IsFirst can partition data and calculate first event 
----IsFirst(timeunit, duration). returns 1 if event is first event given fixed interval
SELECT 
	License_plate,
	Make,
	Time
FROM 
	Input TIMESTAMP BY Time
WHERE 
	IsFirst(minute, 10) = 1


--9. Remove duplicate events in a window
--group by will eliminate any duplicates
WITH Temp AS (
	SELECT Value, DeviceId
	FROM Input TIMESTAMP BY Time
	GROUP BY Value, DeviceId, System.Timestamp()
)
SELECT
	AVG(Value) AS AverageValue, DeviceId
INTO Output
FROM Temp
GROUP BY DeviceId,TumblingWindow(minute, 5)


--10. Detect duration of condition
--suppose a bug caused everything having >20000 pounds, need to detect when start and when end

'''
---input
| Make | Time | Weight |
| --- | --- | --- |
| Make1 |2023-01-01T00:00:01.0000000Z |2000 |
| Make2 |2023-01-01T00:00:02.0000000Z |25000 |
| Make1 |2023-01-01T00:00:03.0000000Z |26000 |
| Make2 |2023-01-01T00:00:04.0000000Z |25000 |
| Make1 |2023-01-01T00:00:05.0000000Z |26000 |
| Make2 |2023-01-01T00:00:06.0000000Z |25000 |
| Make1 |2023-01-01T00:00:07.0000000Z |26000 |
| Make2 |2023-01-01T00:00:08.0000000Z |2000 |

--output
| Start_fault | End_fault |
| --- | --- |
| 2023-01-01T00:00:02.000Z |2023-01-01T00:00:07.000Z |
'''

WITH SelectPreviousEvent AS
(
SELECT
	*,
	LAG([time]) OVER (LIMIT DURATION(hour, 24)) as previous_time,
	LAG([weight]) OVER (LIMIT DURATION(hour, 24)) as previous_weight
FROM input TIMESTAMP BY [time]
)

SELECT 
	LAG(time) OVER (LIMIT DURATION(hour, 24) WHEN previous_weight < 20000 ) [Start_fault],
	previous_time [End_fault]
FROM SelectPreviousEvent
WHERE
	[weight] < 20000
	AND previous_weight > 20000



--11. process events with independent time
'''
Events can arrive late or out of order due to clock skews between event producers, clock skews between partitions, or network latency. 
For example, the device clock for TollID 2 is five seconds behind TollID 1, and the device clock for TollID 3 is 10 seconds behind TollID 1. 
A computation can happen independently for each toll, considering only its own clock data as a timestamp.

'''

'''
Input
| LicensePlate | Make | Time | TollID |
| --- | --- | --- | --- |
| DXE 5291 |Make1 |2023-07-27T00:00:01.0000000Z | 1 |
| YHN 6970 |Make2 |2023-07-27T00:00:05.0000000Z | 1 |
| QYF 9358 |Make1 |2023-07-27T00:00:01.0000000Z | 2 |
| GXF 9462 |Make3 |2023-07-27T00:00:04.0000000Z | 2 |
| VFE 1616 |Make2 |2023-07-27T00:00:10.0000000Z | 1 |
| RMV 8282 |Make1 |2023-07-27T00:00:03.0000000Z | 3 |
| MDR 6128 |Make3 |2023-07-27T00:00:11.0000000Z | 2 |
| YZK 5704 |Make4 |2023-07-27T00:00:07.0000000Z | 3 |

Output
| TollID | Count |
| --- | --- |
| 1 | 2 |
| 2 | 2 |
| 1 | 1 |
| 3 | 1 |
| 2 | 1 |
| 3 | 1 |

'''
SELECT
      TollId,
      COUNT(*) AS Count
FROM input
      TIMESTAMP BY Time OVER TollId
GROUP BY TUMBLINGWINDOW(second, 5), TollId

--Timestamp over by looks at independent toll ID timestamps

--12. Session Windows
--max window size 60 mins, timeout 1 min (window closes if no event in 1 min)
SELECT
	user_id,
	MIN(time) as StartTime,
	MAX(time) as EndTime,
	DATEDIFF(second, MIN(time), MAX(time)) AS duration_in_seconds
FROM input TIMESTAMP BY time
GROUP BY
	user_id,
	SessionWindow(minute, 1, 60) OVER (PARTITION BY user_id)

