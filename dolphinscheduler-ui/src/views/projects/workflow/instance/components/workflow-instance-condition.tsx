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

import { SearchOutlined } from '@vicons/antd'
import {
  NInput,
  NButton,
  NDatePicker,
  NSelect,
  NIcon,
  NSpace,
  NEllipsis
} from 'naive-ui'
import { defineComponent, getCurrentInstance, h, ref, unref } from 'vue'
import { useI18n } from 'vue-i18n'
import { format } from 'date-fns'
import { workflowExecutionStateType } from '@/common/common'
import { queryWorkflowDefinitionList } from '@/service/modules/workflow-definition'
import { SelectMixedOption } from 'naive-ui/lib/select/src/interface'
import { Router, useRouter } from 'vue-router'
import { SelectOption } from 'naive-ui/es/select/src/interface'

export default defineComponent({
  name: 'WorkflowInstanceCondition',
  emits: ['handleSearch'],
  setup(props, ctx) {
    const router: Router = useRouter()

    const searchValRef = ref('')
    const executorNameRef = ref('')
    const dispatchTypeRef = ref()
    const dataRateRef = ref<number | null>(null)
    const hostRef = ref('')
    const stateTypeRef = ref('')
    const startEndTimeRef = ref()
    const projectCode = ref(
      Number(router.currentRoute.value.params.projectCode)
    )
    const workflowDefinitionCodeRef = router.currentRoute.value.query
      .workflowDefineCode
      ? ref(Number(router.currentRoute.value.query.workflowDefineCode))
      : ref()

    const workflowDefinitionOptions = ref<Array<SelectMixedOption>>([])

    const initWorkflowList = (code: number) => {
      queryWorkflowDefinitionList(code).then((result: any) => {
        result.map((item: { code: number; name: string }) => {
          const option: SelectMixedOption = {
            value: item.code,
            label: () => h(NEllipsis, null, item.name),
            filterLabel: item.name
          }
          workflowDefinitionOptions.value.push(option)
        })
      })
    }

    initWorkflowList(projectCode.value)

    const handleSearch = () => {
      let startDate = ''
      let endDate = ''
      if (startEndTimeRef.value) {
        startDate = format(
          new Date(startEndTimeRef.value[0]),
          'yyyy-MM-dd HH:mm:ss'
        )
        endDate = format(
          new Date(startEndTimeRef.value[1]),
          'yyyy-MM-dd HH:mm:ss'
        )
      }

      ctx.emit('handleSearch', {
        searchVal: searchValRef.value,
        executorName: executorNameRef.value,
        dispatchType: dispatchTypeRef.value,
        dataRate: dataRateRef.value,
        host: hostRef.value,
        stateType: stateTypeRef.value,
        startDate,
        endDate,
        workflowDefinitionCode: workflowDefinitionCodeRef.value
      })
    }

    const onClearDispatchType = () => {
      dispatchTypeRef.value = ''
      handleSearch()
    }

    const onClearDataRate = () => {
      dataRateRef.value = null
      handleSearch()
    }

    const onClearSearchVal = () => {
      searchValRef.value = ''
      handleSearch()
    }

    const onClearSearchHost = () => {
      hostRef.value = ''
      handleSearch()
    }

    const onClearSearchExecutor = () => {
      executorNameRef.value = ''
      handleSearch()
    }

    const trim = getCurrentInstance()?.appContext.config.globalProperties.trim

    const selectFilter = (query: string, option: SelectOption) => {
      return option.filterLabel
        ? option.filterLabel
            .toString()
            .toLowerCase()
            .includes(query.toLowerCase())
        : false
    }

    const updateValue = (value: number) => {
      workflowDefinitionCodeRef.value = value
    }

    const updateDispatchType = (value: string) => {
      dispatchTypeRef.value = value
    }

    const validateDataRate = (value: number) => {
      const inputValue = dataRateRef.value
      if (typeof inputValue === 'string') {
        const parsedValue = parseInt(inputValue, 10);
        if (!isNaN(parsedValue) && Number.isInteger(parsedValue)) {
            dataRateRef.value = parsedValue;
            window.$message.warning('已自动调整为整数')
        } else {
            // 输入不是有效的整数，可进行提示或清空输入
            dataRateRef.value = null;
            window.$message.error('请输入有效的整数')
        }
      }
    }

    const generalDispatchList = () => [
      {
        value: 'RANDOM',
        label: 'RANDOM'
      },
      {
        value: 'ROUND_ROBIN',
        label: 'ROUND_ROBIN'
      },
      {
        value: 'HUNGARY',
        label: 'HUNGARY'
      },
      {
        value: 'FLUTTER',
        label: 'FLUTTER'
      },
      {
        value: 'MAX_MIN_FAIRNESS',
        label: 'MAX_MIN_FAIRNESS'
      },
      {
        value: 'BINARY_HUNGARY_GREEDY',
        label: 'BINARY_HUNGARY_GREEDY'
      },
      {
        value: 'FIXED_WEIGHTED_ROUND_ROBIN',
        label: 'FIXED_WEIGHTED_ROUND_ROBIN'
      },
      {
        value: 'DYNAMIC_WEIGHTED_ROUND_ROBIN',
        label: 'DYNAMIC_WEIGHTED_ROUND_ROBIN'
      },
      {
        value: 'WEIGHTED_RANDOM',
        label: 'WEIGHTED_RANDOM'
      },
      {
        value: 'TASK_DATA_SCHEDULE',
        label: 'TASK_DATA_SCHEDULE'
      },
      {
        value: 'ON_LINE_TASK_SCHEDULE',
        label: 'ON_LINE_TASK_SCHEDULE'
      },
      {
        value: 'BINARY_HUNGARY_DFS',
        label: 'BINARY_HUNGARY_DFS'
      },
      {
        value: 'BINARY_HUNGARY_BFS',
        label: 'BINARY_HUNGARY_BFS'
      },
    ]

    return {
      searchValRef,
      executorNameRef,
      dispatchTypeRef,
      dataRateRef,
      hostRef,
      stateTypeRef,
      startEndTimeRef,
      handleSearch,
      onClearDispatchType,
      onClearDataRate,
      onClearSearchVal,
      onClearSearchExecutor,
      onClearSearchHost,
      trim,
      workflowDefinitionOptions: workflowDefinitionOptions,
      workflowDefineCodeRef: workflowDefinitionCodeRef,
      selectFilter,
      updateValue,
      updateDispatchType,
      validateDataRate,
      generalDispatchList
    }
  },
  render() {
    const { t } = useI18n()
    const options = workflowExecutionStateType(t)
    const {
      workflowDefinitionOptions,
      workflowDefineCodeRef,
      selectFilter,
      updateValue,
      updateDispatchType,
      validateDataRate,
    } = this

    return (
      <NSpace justify='end'>
        {h(NSelect, {
          style: {
            width: '200px'
          },
          size: 'small',
          clearable: true,
          filterable: true,
          options: unref(workflowDefinitionOptions),
          value: workflowDefineCodeRef,
          filter: selectFilter,
          onUpdateValue: (value: any) => {
            updateValue(value)
          }
        })}
        <NInput
          allowInput={this.trim}
          size='small'
          v-model:value={this.searchValRef}
          placeholder={t('project.workflow.name')}
          clearable
          onClear={this.onClearSearchVal}
        />
        {h(NSelect, {
          style: {
            width: '200px'
          },
          size: 'small',
          clearable: true,
          filterable: true,
          options: this.generalDispatchList(),
          value: this.dispatchTypeRef,
          filter: selectFilter,
          onUpdateValue: (value: any) => {
            updateDispatchType(value)
          },
          placeholder: '调度策略'
        })}
        <NInput
          allowInput={this.trim}
          size='small'
          style= {{ width: '100px' }}
          v-model:value={this.dataRateRef}
          placeholder={'数据倍率'}
          clearable
          onClear={this.onClearDataRate}
          onBlur={this.validateDataRate}
        />
        <NInput
          allowInput={this.trim}
          size='small'
          v-model:value={this.executorNameRef}
          placeholder={t('project.workflow.executor')}
          clearable
          onClear={this.onClearSearchExecutor}
        />
        <NInput
          allowInput={this.trim}
          size='small'
          v-model:value={this.hostRef}
          placeholder={t('project.workflow.host')}
          clearable
          onClear={this.onClearSearchHost}
        />
        <NSelect
          options={options}
          size='small'
          style={{ width: '200px !important' }}
          defaultValue={''}
          v-model:value={this.stateTypeRef}
        />
        <NDatePicker
          type='datetimerange'
          size='small'
          clearable
          v-model:value={this.startEndTimeRef}
        />
        <NButton type='primary' size='small' onClick={this.handleSearch}>
          <NIcon>
            <SearchOutlined />
          </NIcon>
        </NButton>
      </NSpace>
    )
  }
})
