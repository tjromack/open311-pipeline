# Labeling guide — urgency hand-labels

This set measures how often a human agrees with Claude Haiku 4.5's urgency tier.
The number is only meaningful if the labels are made **blind** and **on the same
evidence the model had**, so:

1. **Don't look at the model's label** — not in DuckDB, not in the fixture, not
   in Langfuse — until `make score-labels` has run. The sheet deliberately omits it.
2. **Use only the columns in the sheet** (`service_name`, `status`, `address`).
   Those are the only fields the classifier's prompt contains
   ([classifier/prompts.py](../classifier/prompts.py) `build_user_prompt`). Don't
   look the address up on a map or search the request ID. You'd be judging on
   information the model never had.
3. **Apply the tier definitions below**, copied verbatim from the classifier's
   system prompt (`PROMPT_VERSION = "v1"`). You're judging the same rubric, not
   your own sense of urgency.
4. **Write a short note** whenever the call is genuinely ambiguous. The notes
   feed the error analysis.
5. Label in one sitting if you can, and commit the filled sheet as-is, before
   scoring. The commit timestamp then shows the labels predate the scores.

## Tiers (from the v1 system prompt)

- **Critical** — imminent risk to life, safety, or critical infrastructure.
  Examples: gas leak, downed power line, water main break flooding a street,
  traffic signal completely out at a busy intersection, sewer backup into a home.
- **High** — likely to cause meaningful harm, injury, or significant property
  damage within 24 hours if not addressed. Examples: streetlight out at a busy
  intersection at night, large pothole on an arterial road, abandoned vehicle
  blocking traffic, rodent infestation in a food establishment.
- **Medium** — quality-of-life issues that meaningfully degrade the neighborhood
  but pose no immediate safety risk. Examples: pothole on a residential side
  street, graffiti on private property, broken sidewalk slab, persistent noise
  complaint, missed garbage pickup.
- **Low** — cosmetic, informational, or low-impact maintenance items. Examples:
  faded street sign, mild graffiti in an alley, request for tree trimming,
  minor park maintenance.

Choose exactly one of `Critical`, `High`, `Medium`, `Low` per row. Case doesn't
matter; the scorer normalises it.

## Workflow

```bash
make label-sheet      # writes eval/labels/label_sheet.csv (50 rows, seeded, blind)
# ...fill in the human_label (and optional notes) column...
make score-labels     # writes eval/RESULTS.md + eval/results.json
```
