class Cassandra

  ## Counters

  # Add a value to the counter in cf:key:super column:column
  def add(column_family, key, value, *columns_and_options)
    column_family, column, sub_column, options = extract_and_validate_params(column_family, key, columns_and_options, WRITE_DEFAULTS)

    mutation_map = if is_super(column_family)
      {
        key => {
          column_family => [_super_counter_mutation(column_family, column, sub_column, value)]
        }
      }
    else
      {
        key => {
          column_family => [_standard_counter_mutation(column_family, column, value)]
        }
      }
    end

    @batch ? @batch << [mutation_map, options[:consistency]] : _mutate(mutation_map, options[:consistency])
  end

  # Increment one or more counters in a single row.
  def add_multiple_columns(column_family, key, hash, options = {})
    column_family, _, _, options = extract_and_validate_params(column_family, key, [options], WRITE_DEFAULTS)

    mutation_map = if is_super(column_family)
      {
        key => {
          column_family => hash.collect do |column, sub_hash|
            sub_hash.collect do |sub_column, value|
              _super_counter_mutation(column_family, column, sub_column, value)
            end
          end.flatten
        }
      }
    else
      {
        key => {
          column_family => hash.collect { |column, value| _standard_counter_mutation(column_family, column, value) }
        }
      }
    end

    @batch ? @batch << [mutation_map, options[:consistency]] : _mutate(mutation_map, options[:consistency])
  end

end
