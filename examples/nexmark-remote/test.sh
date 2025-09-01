for i in $(seq 0 $((100-1))); do
    NODE_ID=$i \
      echo "Starting HolonNode $i…"
done