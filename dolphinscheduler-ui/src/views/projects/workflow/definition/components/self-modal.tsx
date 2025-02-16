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

import { defineComponent, getCurrentInstance, PropType, toRefs } from 'vue'
import { useI18n } from 'vue-i18n'
import Modal from '@/components/modal'
import { useForm } from './use-form'
import { useModal } from './use-modal'
import { NForm, NFormItem, NSelect, NInput, NGradientText } from 'naive-ui'
import { CloudUploadOutlined } from '@vicons/antd'

const props = {
  show: {
    type: Boolean as PropType<boolean>,
    default: false
  }
}

export default defineComponent({
  name: 'workflowDefinitionImport',
  props,
  emits: ['update:show', 'update:row', 'updateList'],
  setup(props, ctx) {
    const { selfState } = useForm()
    const { handleSelfImportDefinition } = useModal(selfState, ctx)
    const hideModal = () => {
      ctx.emit('update:show')
    }
    
    const handleSelfImport = () => {
      handleSelfImportDefinition(selfState.selfForm.type, selfState.selfForm.label, selfState.selfForm.num)
    }

    const trim = getCurrentInstance()?.appContext.config.globalProperties.trim
    const labelOptions = [
      {
        label: 'NoEdge',
        value: 'NoEdge',
      },
      {
        label: 'Montage',
        value: 'Montage',
      },
      {
        label: 'CyberShake',
        value: 'CyberShake',
      },
      {
        label: 'LIGO',
        value: 'LIGO',
      }
    ]

    return {
      hideModal,
      handleSelfImport,
      ...toRefs(selfState),
      trim,
      labelOptions
    }
  },

  render() {
    const { t } = useI18n()

    return (
      <Modal
        show={this.$props.show}
        title={'自定义工作流'}
        onCancel={this.hideModal} // 取消
        onConfirm={this.handleSelfImport} // 确认
        confirmLoading={this.saving}
      >
        <NForm rules={this.selfRules} ref='selfFormRef'>
          <NFormItem label={'任务类型'} path='type'>
            <NInput
              allowInput={this.trim}
              v-model={[this.selfForm.type, 'value']}
              placeholder={''}
              disabled
            />
          </NFormItem>
        </NForm>
        <NForm rules={this.selfRules} ref='selfFormRef'>
          <NFormItem label={'工作流标签'} path='label'>
            <NSelect
              allowInput={this.trim}
              v-model={[this.selfForm.label, 'value']}
              options={this.labelOptions}
              placeholder=""
            />
            {/* <NInput
              allowInput={this.trim}
              v-model={[this.selfForm.label, 'value']}
              placeholder={''}
            /> */}
          </NFormItem>
        </NForm>
        <NForm rules={this.selfRules} ref='selfFormRef'>
          <NFormItem label={'任务数量'} path='num'>
            <NInput
              allowInput={this.trim}
              v-model={[this.selfForm.num, 'value']}
              placeholder={''}
            />
          </NFormItem>
        </NForm>
        <NGradientText style="font-size: 12px; color: #888;">
          由于不同工作流结构不同，实际任务数有偏差
        </NGradientText>
      </Modal>
    )
  }
})
