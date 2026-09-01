defmodule Membrane.MoQ.ExUnitDiffTimeoutCanaryTest do
  # Deliberately failing canary for elixir-lang/elixir#14939: when
  # ExUnit.Diff.compute exceeds its 1.5s budget, find_diff returns nil and
  # Elixir < 1.20 crashes the CLI formatter with a MatchError, silencing the
  # rest of the run. Fixed by 0055f2fe53 (1.20). Expected on a fixed image: a
  # normal failure report with no diff. Remove once CI confirms.
  use ExUnit.Case, async: true

  test "a diff that exceeds the formatter budget is reported, not fatal" do
    left = for i <- 1..300, do: :crypto.strong_rand_bytes(64) <> <<i::32>>
    right = for i <- 1..300, do: :crypto.strong_rand_bytes(64) <> <<i::32>>
    assert left == right
  end
end
