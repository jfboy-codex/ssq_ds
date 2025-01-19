/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { render, h, ref } from 'vue'
import { useRoute } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { tasksState } from '@/common/common'
import { queryTaskListByWorkflowId } from '@/service/modules/workflow-instances'
import NodeStatus from '@/views/projects/workflow/components/dag/dag-node-status'
import { useTaskNodeStore } from '@/store/project/task-node'
import type { IWorkflowTaskInstance, ITaskState } from './types'
import type { Graph } from '@antv/x6'
import type { Ref } from 'vue'
import _ from 'lodash'
import {
  NODE,
  NODE_KEY,
  NODE_STATUS_MARKUP,
  EDGE,
  EDGE_KEY,
} from './dag-config'

interface Options {
  graph: Ref<Graph | undefined>
}

/**
 * Node status and tooltip
 */
export function useNodeStatus(options: Options) {
  const { graph } = options
  const route = useRoute()
  const taskList = ref<Array<IWorkflowTaskInstance>>([])

  const { t } = useI18n()

  const nodeStore = useTaskNodeStore()

  const pathKey = _.merge(_.cloneDeep(EDGE), EDGE_KEY)
  const nodeKey = _.merge(_.cloneDeep(NODE.attrs), NODE_KEY.attrs)

  // ssq 关键路径
  const setEdgeStatus = (
  ) => {
    const edges = graph.value?.getEdges()
    if (edges) {
      edges.forEach((edge) => {
        let isKeyPath = false
        const sourceId = edge.getSourceCell()?.id
        const targetId = edge.getTargetCell()?.id
        const sourceNode = sourceId ? graph.value?.getCellById(sourceId) : undefined
        const targetNode = targetId ? graph.value?.getCellById(targetId) : undefined
        if (sourceNode && targetNode) {
          const sourceTaskInstance = sourceNode.data.taskInstance
          const targetTaskInstance = targetNode.data.taskInstance
          isKeyPath = sourceTaskInstance.nextKeyTask === targetTaskInstance.taskCode
        }
        edge.setData({ keyFlag: isKeyPath ? 1 : 0 })
        // if (isKeyPath) {
        //   edge.setAttrs(pathKey.attrs)
        // }
      })
    }
  }

  const setNodeStatus = (
    code: string,
    state: ITaskState,
    taskInstance: any
  ) => {
    const stateProps = tasksState(t)[state]
    const node = graph.value?.getCellById(code)
    if (node) {
      // Destroy the previous dom
      node.removeMarkup()
      node.setMarkup(NODE.markup.concat(NODE_STATUS_MARKUP))
      node.setData({taskInstance:taskInstance}) // 加入taskInstance数据

      // ssq关键路径
      // if (taskInstance.keyFlag === 1) {
      //   node.setAttrs(nodeKey)
      // }

      const nodeView = graph.value?.findViewByCell(node)
      const el = nodeView?.find('div')[0]
      const a = h(NodeStatus, {
        t,
        taskInstance,
        stateProps
      })

      render(a, el as HTMLElement)
    }
  }

  /**
   * Task status
   */
  const refreshTaskStatus = () => {
    const projectCode = Number(route.params.projectCode)
    const instanceId = Number(route.params.id)

    queryTaskListByWorkflowId(instanceId, projectCode).then((res: any) => {
      window.$message.success(t('project.workflow.refresh_status_succeeded'))
      taskList.value = res.taskList
      if (taskList.value) {
        const allDependentResult = {}
        taskList.value.forEach((taskInstance: any) => {
          // 这个时候节点已经存在，直接更新状态
          setNodeStatus(taskInstance.taskCode, taskInstance.state, taskInstance)
          if (taskInstance.dependentResult) {
            Object.assign(
              allDependentResult,
              JSON.parse(taskInstance.dependentResult)
            )
          }
        })
        nodeStore.updateDependentResult(allDependentResult)
      }
      setEdgeStatus() // ssq关键路径
    })
  }

  return {
    taskList,
    refreshTaskStatus
  }
}
