defmodule BroadwayDashboard.GenStage.PipelineGraph do
  @moduledoc false

  # This module is responsible for building LayeredGraphComponent layers
  # for GenStage pipelines. Unlike Broadway's variable layer structure,
  # GenStage pipelines always have exactly 3 layers:
  # - Producers (top)
  # - Processors (middle)
  # - Broadcasters (bottom)

  alias Phoenix.LiveDashboard.LayeredGraphComponent

  @type topology_workload :: [
          {:producers | :processors | :broadcasters,
           [
             %{
               name: atom(),
               concurrency: pos_integer(),
               data_type: atom(),
               workloads: [non_neg_integer()]
             }
           ]}
        ]

  @doc """
  Builds layers for LayeredGraphComponent from GenStage topology workload.

  ## Parameters

    * `topology_workload` - A keyword list returned by Counters.topology_workload/3.
      Each entry is a tuple of {stage_type, groups} where groups is a list of
      stage configurations with workload data.

  ## Returns

  A list of layers suitable for Phoenix.LiveDashboard.LayeredGraphComponent.
  Each layer is a list of nodes with:
  - `id`: Unique identifier string
  - `children`: List of child node ids from the next layer down
  - `data`: Either a map with :label and :detail (for stages with workload)
           or a string (for producers without workload)

  ## Examples

      iex> topology_workload = [
      ...>   producers: [
      ...>     %{name: :producer, concurrency: 1, data_type: :fills, workloads: []}
      ...>   ],
      ...>   processors: [
      ...>     %{name: :processor, concurrency: 2, data_type: :fills, workloads: [10, 25]},
      ...>     %{name: :processor, concurrency: 2, data_type: :trades, workloads: [5, 15]}
      ...>   ],
      ...>   broadcasters: [
      ...>     %{name: :broadcaster, concurrency: 1, data_type: :fills, workloads: [50]},
      ...>     %{name: :broadcaster, concurrency: 1, data_type: :trades, workloads: [60]}
      ...>   ]
      ...> ]
      iex> BroadwayDashboard.GenStage.PipelineGraph.build_layers(topology_workload)
      [
        [%{id: "broadcaster_fills_0", children: [], data: %{label: "bcast_fills_0", detail: 50}}, ...],
        [%{id: "processor_fills_0", children: ["broadcaster_fills_0", ...], data: %{label: "proc_fills_0", detail: 10}}, ...],
        [%{id: "producer_0", children: ["processor_fills_0", ...], data: "prod_0"}, ...]
      ]
  """
  @spec build_layers(topology_workload()) :: [LayeredGraphComponent.layer()]
  def build_layers(topology_workload) when is_list(topology_workload) do
    # Always 3 layers for GenStage: broadcasters (bottom), processors (middle), producers (top)
    steps = [:broadcasters, :processors, :producers]
    build_layers(topology_workload, steps, [])
  end

  defp build_layers(_topology, [], result), do: result

  defp build_layers(topology, [step | rest], result) do
    previous_layer = List.first(result) || []

    layer =
      case step do
        :producers ->
          build_producer_nodes(topology[:producers], previous_layer)

        :processors ->
          build_stage_nodes(topology[:processors], "proc", previous_layer)

        :broadcasters ->
          build_stage_nodes(topology[:broadcasters], "bcast", previous_layer)
      end

    build_layers(topology, rest, [layer | result])
  end

  defp build_stage_nodes(stage_groups, prefix, children_layer) do
    for group <- stage_groups || [],
        i <- 0..(group.concurrency - 1) do
      %{
        id: "#{group.name}_#{group.data_type}_#{i}",
        children: Enum.map(children_layer, & &1.id),
        data: %{
          label: "#{prefix}_#{group.data_type}_#{i}",
          detail: Enum.at(group.workloads, i, 0)
        }
      }
    end
  end

  defp build_producer_nodes(producers, children_layer) do
    for producer <- producers || [],
        i <- 0..(producer.concurrency - 1) do
      %{
        id: "#{producer.name}_#{i}",
        children: Enum.map(children_layer, & &1.id),
        data: "prod_#{i}"
      }
    end
  end
end
