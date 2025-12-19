defmodule BroadwayDashboard.GenStage.Telemetry do
  @moduledoc false

  # Measurements of GenStage pipelines are based on telemetry events.
  #
  # The load of a stage is calculated based on the time it took from
  # the last execution to the current one. If this time is shorter,
  # it means that the stage is doing more work.
  #
  # GenStage pipelines partition work by both stage concurrency AND
  # data types (e.g., :fills, :trades, :order_statuses).

  alias BroadwayDashboard.GenStage.Counters

  @events [
    [:node_watcher, :pipeline, :init],
    [:node_watcher, :pipeline, :processor, :start],
    [:node_watcher, :pipeline, :processor, :stop],
    [:node_watcher, :pipeline, :broadcaster, :start],
    [:node_watcher, :pipeline, :broadcaster, :stop]
  ]

  @doc """
  Attaches telemetry handlers for a GenStage pipeline.

  ## Parameters

    * `parent` - The parent process PID (used for unique handler ID)
    * `pipeline` - The pipeline name atom to filter events
    * `counters` - The Counters struct for storing metrics

  ## Returns

  `:ok`
  """
  def attach(parent, pipeline, counters) do
    :telemetry.attach_many(
      {__MODULE__, parent},
      @events,
      &__MODULE__.handle_event/4,
      {parent, pipeline, counters}
    )
  end

  @doc """
  Detaches all telemetry handlers for a parent process.

  ## Parameters

    * `parent` - The parent process PID

  ## Returns

  `:ok`
  """
  def detach(parent) do
    :telemetry.detach({__MODULE__, parent})
  end

  # Handles pipeline initialization event.
  # Signals the Metrics server to reinitialize counters when the pipeline restarts.
  def handle_event(
        [:node_watcher, :pipeline, :init],
        _measurements,
        %{pipeline: event_pipeline} = _metadata,
        {parent, pipeline, _counters}
      )
      when event_pipeline == pipeline do
    send(parent, {:pipeline_restarted, pipeline})
    :ok
  end

  # Handles stage start events for processors and broadcasters.
  # Records the monotonic start time for the stage instance.
  def handle_event(
        [:node_watcher, :pipeline, stage_type, :start],
        _measurements,
        %{pipeline: event_pipeline} = metadata,
        {_parent, pipeline, counters}
      )
      when event_pipeline == pipeline and stage_type in [:processor, :broadcaster] do
    # Fetch monotonic time (will be provided by telemetry in future versions)
    monotonic_time = System.monotonic_time()

    # Extract metadata
    data_type = metadata.data_type
    stage_index = metadata.stage_index

    # Normalize stage_type to plural form used by Counters
    stage_type_plural = stage_type_to_plural(stage_type)

    # Record start time
    :ok = Counters.put_stage_start(counters, stage_type_plural, data_type, stage_index, monotonic_time)
  end

  # Handles stage stop events for processors and broadcasters.
  # Calculates workload percentage and updates counters with:
  # - End time
  # - Workload percentage
  # - Success/failure counts
  def handle_event(
        [:node_watcher, :pipeline, stage_type, :stop],
        measurements,
        %{pipeline: event_pipeline} = metadata,
        {_parent, pipeline, counters}
      )
      when event_pipeline == pipeline and stage_type in [:processor, :broadcaster] do
    # Fetch monotonic time
    monotonic_time = System.monotonic_time()

    # Extract metadata
    data_type = metadata.data_type
    stage_index = metadata.stage_index
    duration = measurements.duration

    # Normalize stage_type to plural form used by Counters
    stage_type_plural = stage_type_to_plural(stage_type)

    # Fetch timing information
    {:ok, last_end_time} = Counters.fetch_stage_end(counters, stage_type_plural, data_type, stage_index)
    {:ok, start_time} = Counters.fetch_stage_start(counters, stage_type_plural, data_type, stage_index)

    # Calculate workload percentage
    workload = calc_workload(start_time, last_end_time, duration)

    # Update counters
    :ok = Counters.put_stage_end(counters, stage_type_plural, data_type, stage_index, monotonic_time)
    :ok = Counters.put_stage_workload(counters, stage_type_plural, data_type, stage_index, workload)

    # Update success/failure counts
    successful = measurements[:successful] || 0
    failed = measurements[:failed] || 0
    :ok = Counters.incr(counters, successful, failed)
  end

  # Ignore events from other pipelines or unhandled event types
  def handle_event(_, _, _, _), do: :ok

  # Calculates workload percentage based on idle time and processing duration.
  #
  # Workload is the percentage of time spent processing vs. total time (idle + processing).
  # Returns a value from 0 to 100.
  #
  # Edge cases:
  # - If duration is 0, workload is 0 (no work done)
  # - If this is the first event (last_end_time = 0), idle_time is clamped to 0
  # - Negative idle_time (timing issues) is clamped to 0
  defp calc_workload(start_time, last_end_time, duration) do
    if duration > 0 do
      idle_time = start_time - last_end_time
      # Clamp idle_time to 0 if negative (first event or timing issue)
      idle_time = max(0, idle_time)
      round(duration / (idle_time + duration) * 100)
    else
      0
    end
  end

  # Converts singular stage type atoms to plural form used by Counters
  defp stage_type_to_plural(:processor), do: :processors
  defp stage_type_to_plural(:broadcaster), do: :broadcasters
end
