start=1
end=100

for idx in $(seq "$start" "$end"); do
  python fires/run_budget_day.py --day-index "$idx"
done


