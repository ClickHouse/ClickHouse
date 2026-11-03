-- sequenceMatchEvents must return the events of the chain that matched the pattern. It used to keep the
-- longest chain seen over all the backtracking attempts, so when an abandoned attempt (here, one that
-- failed a time condition after its last event) was as long as the matching chain, its events were
-- returned instead. Each case is followed by a control without the abandoned attempt.

SELECT 'time condition after the only event';
SELECT sequenceMatch('(?1)(?t==2)')(t, c = 1, c = 2), sequenceMatchEvents('(?1)(?t==2)')(t, c = 1, c = 2)
FROM values('t DateTime(\'UTC\'), c UInt8', (0, 1), (3, 1), (5, 2));
SELECT sequenceMatch('(?1)(?t==2)')(t, c = 1, c = 2), sequenceMatchEvents('(?1)(?t==2)')(t, c = 1, c = 2)
FROM values('t DateTime(\'UTC\'), c UInt8', (3, 1), (5, 2));

SELECT 'time condition after two events';
SELECT sequenceMatch('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3), sequenceMatchEvents('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3)
FROM values('t DateTime(\'UTC\'), c UInt8', (0, 1), (1, 2), (10, 1), (11, 2), (13, 3));
SELECT sequenceMatch('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3), sequenceMatchEvents('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3)
FROM values('t DateTime(\'UTC\'), c UInt8', (10, 1), (11, 2), (13, 3));

SELECT 'a last time condition that backtracks right away';
SELECT sequenceMatch('(?1)(?2)(?t<=1)')(t, c = 1, c = 2, c = 3), sequenceMatchEvents('(?1)(?2)(?t<=1)')(t, c = 1, c = 2, c = 3)
FROM values('t DateTime(\'UTC\'), c UInt8', (0, 1), (1, 2), (5, 1), (6, 2), (7, 3));
SELECT sequenceMatch('(?1)(?2)(?t<=1)')(t, c = 1, c = 2, c = 3), sequenceMatchEvents('(?1)(?2)(?t<=1)')(t, c = 1, c = 2, c = 3)
FROM values('t DateTime(\'UTC\'), c UInt8', (5, 1), (6, 2), (7, 3));

-- Without a match the longest partial chain is returned, the first one among those of equal length.
SELECT 'no match';
SELECT sequenceMatch('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3), sequenceMatchEvents('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3)
FROM values('t DateTime(\'UTC\'), c UInt8', (0, 1), (10, 1), (11, 2), (20, 3));
SELECT sequenceMatch('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3), sequenceMatchEvents('(?1)(?2)(?t==2)')(t, c = 1, c = 2, c = 3)
FROM values('t DateTime(\'UTC\'), c UInt8', (0, 1), (1, 2), (10, 1), (11, 2), (20, 3));
