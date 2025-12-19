defmodule BroadwayDashboard.GenStage.Counters do
  @moduledoc """
  Atomics-based metrics storage for GenStage pipelines with data type partitioning.

  This module extends the Broadway counters pattern to support GenStage pipelines that
  partition work by both stage concurrency AND data types. For example, a pipeline with
  10 processors handling 3 data types (:fills, :trades, :order_statuses) would have
  30 processor stage slots (10 × 3).

  ## Structure

  The atomics array is partitioned into three regions:
  - Slots 0 to N-1: start times (monotonic timestamps)
  - Slots N to 2N-1: end times (monotonic timestamps)
  - Slots 2N to 3N-1: workload percentages (0-100)

  Where N is the total number of {stage_type, data_type, index} combinations.

  ## Example

  For a topology with:
  - 10 processors
  - 5 broadcasters
  - 3 data types (:fills, :trades, :order_statuses)

  Total stages = 3 × (10 + 5) = 45
  Atomic slots = 45 × 3 = 135

  Position mapping examples:
  - {:processors, :fills, 0} → position 0
  - {:processors, :fills, 1} → position 1
  - {:processors, :trades, 0} → position 10
  - {:broadcasters, :fills, 0} → position 30
  """

  defstruct stages: 0,
            counters: nil,
            atomics: nil,
            stage_positions: %{}

  @type stage_type :: :producers | :processors | :broadcasters | atom()
  @type data_type :: atom()
  @type index :: non_neg_integer()
  @type position :: non_neg_integer()
  @type value :: integer()

  @type t :: %__MODULE__{
          stages: non_neg_integer(),
          counters: :counters.counters_ref(),
          atomics: :atomics.atomics_ref(),
          stage_positions: %{{stage_type(), data_type(), index()} => position()}
        }

  @type topology :: [{stage_type(), [stage_config()]}]
  @type stage_config :: %{name: atom(), concurrency: pos_integer()}

  @doc """
  Builds a counters struct based on a GenStage topology and data types list.

  ## Parameters

    * `topology` - A keyword list of stage configurations, e.g.:
      ```
      [
        producers: [%{name: :producer, concurrency: 1}],
        processors: [%{name: :processor, concurrency: 10}],
        broadcasters: [%{name: :broadcaster, concurrency: 5}]
      ]
      ```

    * `data_types` - A list of data type atoms, e.g.:
      `[:fills, :trades, :order_statuses]`

  ## Returns

  A `%Counters{}` struct with allocated atomics and position mappings.

  ## Examples

      iex> topology = [
      ...>   processors: [%{name: :processor, concurrency: 2}]
      ...> ]
      iex> data_types = [:fills, :trades]
      iex> counters = BroadwayDashboard.GenStage.Counters.build(topology, data_types)
      iex> counters.stages
      4
  """
  @spec build(topology(), [data_type()]) :: t()
  def build(topology, data_types) do
    positions = calculate_positions(topology, data_types)
    total = map_size(positions)

    # :atomics.new/2 requires size >= 1, so we use max(1, total * 3)
    atomics_size = max(1, total * 3)

    %__MODULE__{
      stages: total,
      counters: :counters.new(2, [:write_concurrency]),
      atomics: :atomics.new(atomics_size, signed: true),
      stage_positions: positions
    }
  end

  @doc """
  Increments the total of successful and failed events.

  ## Parameters

    * `counters` - The counters struct
    * `successes` - Number of successful events to add
    * `failures` - Number of failed events to add

  ## Returns

  `:ok`

  ## Examples

      iex> counters = build(topology, [:fills])
      iex> BroadwayDashboard.GenStage.Counters.incr(counters, 10, 2)
      :ok
  """
  @spec incr(t(), non_neg_integer(), non_neg_integer()) :: :ok
  def incr(%__MODULE__{} = counters, successes, failures) do
    :ok = :counters.add(counters.counters, 1, successes)
    :ok = :counters.add(counters.counters, 2, failures)
  end

  @doc """
  Counts the successful and failed events.

  ## Returns

  `{:ok, {successful, failed}}` where both are non-negative integers.

  ## Examples

      iex> counters = build(topology, [:fills])
      iex> incr(counters, 100, 5)
      iex> BroadwayDashboard.GenStage.Counters.count(counters)
      {:ok, {100, 5}}
  """
  @spec count(t()) :: {:ok, {non_neg_integer(), non_neg_integer()}}
  def count(%__MODULE__{} = counters) do
    successful = :counters.get(counters.counters, 1)
    failed = :counters.get(counters.counters, 2)
    {:ok, {successful, failed}}
  end

  @doc """
  Returns the topology with workload data for each stage, grouped by data type.

  The workload is a number from 0 to 100 representing the percentage of time a
  stage is busy processing events.

  ## Parameters

    * `counters` - The counters struct
    * `topology` - The GenStage topology
    * `data_types` - List of data types to group by

  ## Returns

  A keyword list matching the topology structure, with each stage type containing
  a list of groups (one per data type), each with a `:workloads` list.

  ## Examples

      iex> topology = [
      ...>   processors: [%{name: :processor, concurrency: 2}]
      ...> ]
      iex> data_types = [:fills, :trades]
      iex> counters = build(topology, data_types)
      iex> topology_workload(counters, topology, data_types)
      [
        processors: [
          %{name: :processor, data_type: :fills, concurrency: 2, workloads: [0, 0]},
          %{name: :processor, data_type: :trades, concurrency: 2, workloads: [0, 0]}
        ]
      ]
  """
  @spec topology_workload(t(), topology(), [data_type()]) :: topology()
  def topology_workload(%__MODULE__{} = counters, topology, data_types) do
    for {stage_type, [config]} <- topology do
      groups =
        for data_type <- data_types do
          workloads =
            for index <- 0..(config.concurrency - 1) do
              {:ok, value} = fetch_stage_workload(counters, stage_type, data_type, index)
              value
            end

          config
          |> Map.put(:data_type, data_type)
          |> Map.put(:workloads, workloads)
        end

      {stage_type, groups}
    end
  end

  ## Stage start times

  @doc """
  Stores the start time for a specific stage instance.

  ## Parameters

    * `counters` - The counters struct
    * `stage_type` - The stage type (e.g., `:processors`, `:broadcasters`)
    * `data_type` - The data type being processed
    * `index` - The stage index (0..concurrency-1)
    * `value` - The monotonic timestamp

  ## Returns

  `:ok` if the position exists, `{:error, :position_not_found}` otherwise.
  """
  @spec put_stage_start(t(), stage_type(), data_type(), index(), value()) ::
          :ok | {:error, :position_not_found}
  def put_stage_start(%__MODULE__{} = counters, stage_type, data_type, index, value)
      when is_atom(stage_type) and is_atom(data_type) and is_integer(index) and
             is_integer(value) do
    with {:ok, pos} <- get_position(counters, stage_type, data_type, index) do
      :atomics.put(counters.atomics, pos + 1, value)
    end
  end

  @doc """
  Fetches the start time for a specific stage instance.

  ## Returns

  `{:ok, value}` if the position exists, `{:error, :position_not_found}` otherwise.
  """
  @spec fetch_stage_start(t(), stage_type(), data_type(), index()) ::
          {:ok, value()} | {:error, :position_not_found}
  def fetch_stage_start(%__MODULE__{} = counters, stage_type, data_type, index)
      when is_atom(stage_type) and is_atom(data_type) and is_integer(index) do
    with {:ok, pos} <- get_position(counters, stage_type, data_type, index) do
      {:ok, :atomics.get(counters.atomics, pos + 1)}
    end
  end

  ## Stage end times

  @doc """
  Stores the end time for a specific stage instance.

  ## Parameters

    * `counters` - The counters struct
    * `stage_type` - The stage type (e.g., `:processors`, `:broadcasters`)
    * `data_type` - The data type being processed
    * `index` - The stage index (0..concurrency-1)
    * `value` - The monotonic timestamp

  ## Returns

  `:ok` if the position exists, `{:error, :position_not_found}` otherwise.
  """
  @spec put_stage_end(t(), stage_type(), data_type(), index(), value()) ::
          :ok | {:error, :position_not_found}
  def put_stage_end(%__MODULE__{} = counters, stage_type, data_type, index, value)
      when is_atom(stage_type) and is_atom(data_type) and is_integer(index) and
             is_integer(value) do
    with {:ok, pos} <- get_position(counters, stage_type, data_type, index) do
      :atomics.put(counters.atomics, counters.stages + pos + 1, value)
    end
  end

  @doc """
  Fetches the end time for a specific stage instance.

  ## Returns

  `{:ok, value}` if the position exists, `{:error, :position_not_found}` otherwise.
  """
  @spec fetch_stage_end(t(), stage_type(), data_type(), index()) ::
          {:ok, value()} | {:error, :position_not_found}
  def fetch_stage_end(%__MODULE__{} = counters, stage_type, data_type, index)
      when is_atom(stage_type) and is_atom(data_type) and is_integer(index) do
    with {:ok, pos} <- get_position(counters, stage_type, data_type, index) do
      {:ok, :atomics.get(counters.atomics, counters.stages + pos + 1)}
    end
  end

  ## Stage workload percentages

  @doc """
  Stores the workload percentage for a specific stage instance.

  ## Parameters

    * `counters` - The counters struct
    * `stage_type` - The stage type (e.g., `:processors`, `:broadcasters`)
    * `data_type` - The data type being processed
    * `index` - The stage index (0..concurrency-1)
    * `value` - The workload percentage (0-100)

  ## Returns

  `:ok` if the position exists, `{:error, :position_not_found}` otherwise.
  """
  @spec put_stage_workload(t(), stage_type(), data_type(), index(), value()) ::
          :ok | {:error, :position_not_found}
  def put_stage_workload(%__MODULE__{} = counters, stage_type, data_type, index, value)
      when is_atom(stage_type) and is_atom(data_type) and is_integer(index) and
             is_integer(value) do
    with {:ok, pos} <- get_position(counters, stage_type, data_type, index) do
      :atomics.put(counters.atomics, counters.stages * 2 + pos + 1, value)
    end
  end

  @doc """
  Fetches the workload percentage for a specific stage instance.

  ## Returns

  `{:ok, value}` if the position exists, `{:error, :position_not_found}` otherwise.
  """
  @spec fetch_stage_workload(t(), stage_type(), data_type(), index()) ::
          {:ok, value()} | {:error, :position_not_found}
  def fetch_stage_workload(%__MODULE__{} = counters, stage_type, data_type, index)
      when is_atom(stage_type) and is_atom(data_type) and is_integer(index) do
    with {:ok, pos} <- get_position(counters, stage_type, data_type, index) do
      {:ok, :atomics.get(counters.atomics, counters.stages * 2 + pos + 1)}
    end
  end

  ## Private functions

  # Calculates position mappings for all {stage_type, data_type, index} combinations.
  #
  # The positions are assigned deterministically by sorting the keys to ensure
  # consistent mapping across builds.
  @spec calculate_positions(topology(), [data_type()]) :: %{
          {stage_type(), data_type(), index()} => position()
        }
  defp calculate_positions(topology, data_types) do
    # Flatten topology to get all stage combinations
    stages =
      for {stage_type, [config]} <- topology,
          data_type <- data_types,
          index <- 0..(config.concurrency - 1) do
        {stage_type, data_type, index}
      end

    # Assign positions with deterministic ordering
    stages
    |> Enum.sort()
    |> Enum.with_index()
    |> Map.new()
  end

  # Retrieves the position for a given stage key.
  @spec get_position(t(), stage_type(), data_type(), index()) ::
          {:ok, position()} | {:error, :position_not_found}
  defp get_position(%__MODULE__{} = counters, stage_type, data_type, index) do
    key = {stage_type, data_type, index}

    case Map.fetch(counters.stage_positions, key) do
      {:ok, pos} -> {:ok, pos}
      :error -> {:error, :position_not_found}
    end
  end
end
