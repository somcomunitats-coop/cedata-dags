-- https://dbdiagram.io/d/61a75a848c901501c0db398a

CREATE TABLE "curveregistry" (
  "ts" timestamp,
  "meter" varchar,
  "contract" varchar,
  "input_active_energy_kwh" bigint,
  "output_active_energy_kwh" bigint,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE INDEX ON "curveregistry" ("ts", "meter");




create table "stg_contracts" (
)