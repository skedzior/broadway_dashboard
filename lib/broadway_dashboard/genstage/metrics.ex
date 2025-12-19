defmodule BroadwayDashboard.GenStage.Metrics do
  @moduledoc false

  # GenServer that manages listener registration, periodic refresh,
  # and metrics payload broadcasting for GenStage pipelines.
  #
  # Lifecycle:
  # 1. Started on-demand when first LiveView client connects
  # 2. Attaches telemetry handlers
  # 3. Polls metrics every 1 second
  # 4. Broadcasts `{:update_pipeline, payload}` to all listeners
  # 5. Shuts down 5 seconds after last listener disconnects

  use GenServer

  alias BroadwayDashboard.GenStage.{Counters, Telemetry}

  @default_interval 1_000
  @shutdown_delay 5_000

  defstruct [
    :pipeline,
    :data_types,
    :topology,
    :counters,
    :interval,
    :timer,
    :shutdown_timer,
    listeners: %{}
  ]

  @doc """
  Registers a parent process as a listener for pipeline metrics.

  ## Parameters

    * `target_node` - The node where the pipeline is running
    * `parent` - The parent process PID (typically a LiveView)
    * `pipeline` - The pipeline name atom
    * `opts` - Options keyword list with:
      * `:data_types` - List of data type atoms (required)
      * `:topology` - Topology keyword list (required)
      * `:interval` - Refresh interval in milliseconds (optional, default: 1000)

  ## Returns

    * `{:ok, initial_payload}` - Successfully registered with initial metrics
    * `{:error, :pipeline_not_found}` - Pipeline doesn't exist on target node
    * `{:error, reason}` - Other errors

  ## Examples

      iex> opts = [
      ...>   data_types: [:fills, :trades],
      ...>   topology: [
      ...>     processors: [%{name: :processor, concurrency: 10}],
      ...>     broadcasters: [%{name: :broadcaster, concurrency: 5}]
      ...>   ]
      ...> ]
      iex> Metrics.listen(node(), self(), MyPipeline, opts)
      {:ok, %{pipeline: MyPipeline, topology_workload: [...], successful: 0, failed: 0}}
  """
  def listen(target_node, parent, pipeline, opts) do
    name = server_name(pipeline)

    with :ok <- check_pipeline_running_at_node(pipeline, target_node),
         {:ok, server_name} <-
           ensure_server_started_at_node(pipeline, name, target_node, opts) do
      GenServer.call(server_name, {:listen, parent})
    end
  end

  @doc """
  Returns the unique server name for a GenStage pipeline's Metrics GenServer.

  ## Parameters

    * `pipeline` - The pipeline name (atom or via tuple)

  ## Returns

  An atom representing the unique server name.

  ## Examples

      iex> Metrics.server_name(MyPipeline)
      :genstage_dashboard_metrics_MyPipeline

      iex> Metrics.server_name({:via, Registry, {:my_registry, :key}})
      :"genstage_dashboard_metrics_Registry_{:my_registry, :key}"
  """
  def server_name(pipeline) when is_atom(pipeline) do
    :"genstage_dashboard_metrics_#{inspect(pipeline)}"
  end

  def server_name({:via, registry, term}) when is_atom(registry) do
    :"genstage_dashboard_metrics_#{inspect(registry)}_#{inspect(term)}"
  end

  @doc """
  Starts the Metrics GenServer.

  ## Parameters

    * `opts` - Keyword list with:
      * `:name` - Server name (required)
      * `:pipeline` - Pipeline name (required)
      * `:data_types` - List of data types (required)
      * `:topology` - Topology configuration (required)
      * `:interval` - Refresh interval in ms (optional, default: 1000)

  ## Returns

    * `{:ok, pid}` - Successfully started
    * `{:error, {:already_started, pid}}` - Server already running
  """
  def start(opts) do
    GenServer.start(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Starts the Metrics GenServer as part of a supervision tree.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    pipeline = Keyword.fetch!(opts, :pipeline)
    data_types = Keyword.fetch!(opts, :data_types)
    topology = Keyword.fetch!(opts, :topology)
    interval = Keyword.get(opts, :interval, @default_interval)

    # Build counters with GenStage's data type partitioning
    counters = Counters.build(topology, data_types)

    # Attach telemetry handlers
    :ok = Telemetry.attach(self(), pipeline, counters)

    state = %__MODULE__{
      pipeline: pipeline,
      data_types: data_types,
      topology: topology,
      counters: counters,
      interval: interval,
      timer: schedule_refresh(interval)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:listen, parent}, _from, state) do
    # Monitor the parent process
    ref = Process.monitor(parent)

    # Add to listeners map
    listeners = Map.put(state.listeners, ref, parent)

    # Cancel any pending shutdown timer
    shutdown_timer =
      if state.shutdown_timer do
        Process.cancel_timer(state.shutdown_timer)
        nil
      else
        nil
      end

    # Build initial payload
    payload = build_update_payload(state)

    {:reply, {:ok, payload}, %{state | listeners: listeners, shutdown_timer: shutdown_timer}}
  end

  @impl true
  def handle_info(:refresh, state) do
    # Build update payload
    payload = build_update_payload(state)

    # Send to all listeners
    for pid <- Map.values(state.listeners) do
      send(pid, {:update_pipeline, payload})
    end

    # Schedule next refresh
    {:noreply, %{state | timer: schedule_refresh(state.interval)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    # Remove listener from map
    listeners = Map.delete(state.listeners, ref)

    # Schedule shutdown if no listeners remain
    shutdown_timer =
      if listeners == %{} do
        Process.send_after(self(), :shutdown, @shutdown_delay)
      else
        state.shutdown_timer
      end

    {:noreply, %{state | listeners: listeners, shutdown_timer: shutdown_timer}}
  end

  @impl true
  def handle_info(:shutdown, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:pipeline_restarted, pipeline}, state) when pipeline == state.pipeline do
    # Rebuild counters when pipeline restarts
    counters = Counters.build(state.topology, state.data_types)

    # Detach old handlers and attach new ones
    Telemetry.detach(self())
    :ok = Telemetry.attach(self(), pipeline, counters)

    {:noreply, %{state | counters: counters}}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    # Detach telemetry handlers
    Telemetry.detach(self())
    :ok
  end

  ## Private Functions

  defp check_pipeline_running_at_node(pipeline, target_node) do
    result =
      if target_node == node() do
        GenServer.whereis(pipeline)
      else
        :rpc.call(target_node, GenServer, :whereis, [pipeline])
      end

    case result do
      pid when is_pid(pid) ->
        :ok

      _ ->
        {:error, :pipeline_not_found}
    end
  end

  defp ensure_server_started_at_node(pipeline, name, target_node, opts)
       when target_node == node() do
    if GenServer.whereis(name) do
      {:ok, name}
    else
      start_opts = [
        pipeline: pipeline,
        data_types: Keyword.fetch!(opts, :data_types),
        topology: Keyword.fetch!(opts, :topology),
        interval: Keyword.get(opts, :interval, @default_interval),
        name: name
      ]

      with {:ok, _pid} <- start(start_opts) do
        {:ok, name}
      end
    end
  end

  defp ensure_server_started_at_node(_pipeline, name, target_node, _opts) do
    case :rpc.call(target_node, GenServer, :whereis, [name]) do
      pid when is_pid(pid) ->
        {:ok, {name, target_node}}

      nil ->
        # Note: Teleportation would be implemented here for remote nodes
        # For now, we only support local nodes
        {:error, :remote_nodes_not_supported}

      {:badrpc, _} = error ->
        {:error, error}
    end
  end

  defp schedule_refresh(interval) do
    Process.send_after(self(), :refresh, interval)
  end

  defp build_update_payload(state) do
    topology_workload = Counters.topology_workload(state.counters, state.topology, state.data_types)
    {:ok, {successful, failed}} = Counters.count(state.counters)

    %{
      pipeline: state.pipeline,
      topology_workload: topology_workload,
      successful: successful,
      failed: failed
    }
  end
end
