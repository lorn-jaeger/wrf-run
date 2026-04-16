start=100
end=200

for idx in $(seq "$start" "$end"); do
  python fires/run_budget_day.py --day-index "$idx"
done


