start=3
end=10
for idx in $(seq "$start" "$end"); do
  python fires/run_budget_day.py --day-index "$idx" || break
done


