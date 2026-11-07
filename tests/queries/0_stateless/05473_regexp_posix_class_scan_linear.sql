-- Inside a character class, every `[:` makes the regexp analyzer look for a closing `:]`. Without one
-- left in the pattern, each `[:` rescanned the rest of it, so analysis was quadratic and took tens of
-- seconds here before re2 was even built.

SELECT match(materialize('x'), concat('*[', repeat('[:', 250000), 'x]')); -- { serverError CANNOT_COMPILE_REGEXP }

-- The same `[:` run in a valid class: with no `:]` after them, each `[` and `:` is a literal member.
SELECT match(materialize(':x'), concat('[', repeat('[:', 250000), 'a]x')), match(materialize('bx'), concat('[', repeat('[:', 250000), 'a]x'));

-- A named class earlier in the pattern is still recognized, and the `[:` after its `:]` are still literals.
SELECT match(materialize('a:x'), concat('[[:alpha:]][', repeat('[:', 250000), 'a]x')), match(materialize('1:x'), concat('[[:alpha:]][', repeat('[:', 250000), 'a]x'));

-- A `:]` after the run turns the first `[:` into a named class spanning the run, which re2 rejects.
SELECT match(materialize('x'), concat('[', repeat('[:', 1000), ':]]x')); -- { serverError CANNOT_COMPILE_REGEXP }
