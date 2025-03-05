import * as React from 'react';
import { Form, FormGroup, FormSection } from '@patternfly/react-core';
import { useLocation } from 'react-router-dom';
import { usePipelinesAPI } from '~/concepts/pipelines/context';
import { getDisplayNameFromK8sResource } from '~/concepts/k8s/utils';
import {
  FineTuneTaxonomyFormData,
  ModelCustomizationFormData,
} from '~/concepts/pipelines/content/modelCustomizationForm/modelCustomizationFormSchema/validationUtils';
import {
  FineTunePageSections,
  fineTunePageSectionTitles,
} from '~/pages/pipelines/global/modelCustomization/const';
import { UpdateObjectAtPropAndValue } from '~/pages/projects/types';
import FineTunePageFooter from '~/pages/pipelines/global/modelCustomization/FineTunePageFooter';
import BaseModelSection from '~/pages/pipelines/global/modelCustomization/baseModelSection/BaseModelSection';
import TeacherModelSection from '~/pages/pipelines/global/modelCustomization/teacherJudgeSection/TeacherModelSection';
import JudgeModelSection from '~/pages/pipelines/global/modelCustomization/teacherJudgeSection/JudgeModelSection';
import { PipelineKF, PipelineVersionKF } from '~/concepts/pipelines/kfTypes';
import { ModelCustomizationRouterState } from '~/routes';
import { FineTuneTaxonomySection } from './FineTuneTaxonomySection';

type FineTunePageProps = {
  canSubmit: boolean;
  onSuccess: () => void;
  data: ModelCustomizationFormData;
  setData: UpdateObjectAtPropAndValue<ModelCustomizationFormData>;
  ilabPipeline: PipelineKF | null;
  ilabPipelineVersion: PipelineVersionKF | null;
};

const FineTunePage: React.FC<FineTunePageProps> = ({
  canSubmit,
  onSuccess,
  data,
  setData,
  ilabPipeline,
  ilabPipelineVersion,
}) => {
  const projectDetailsDescription = 'This project is used for running your pipeline';
  const { project } = usePipelinesAPI();
  const { state }: { state?: ModelCustomizationRouterState } = useLocation();

  return (
    <Form data-testid="fineTunePageForm">
      <FormSection
        id={FineTunePageSections.PROJECT_DETAILS}
        title={fineTunePageSectionTitles[FineTunePageSections.PROJECT_DETAILS]}
      >
        {projectDetailsDescription}
        <FormGroup
          label="Data Science Project"
          fieldId="model-customization-projectName"
          isRequired
        >
          <div>{getDisplayNameFromK8sResource(project)}</div>
        </FormGroup>
      </FormSection>
      <BaseModelSection
        data={data.baseModel}
        setData={(baseModelData) => setData('baseModel', baseModelData)}
        registryName={state?.modelRegistryDisplayName}
        inputModelName={state?.registeredModelName}
        inputModelVersionName={state?.modelVersionName}
      />
      <FineTuneTaxonomySection
        data={data.taxonomy}
        setData={(dataTaxonomy: FineTuneTaxonomyFormData) => {
          setData('taxonomy', dataTaxonomy);
        }}
      />
      <TeacherModelSection
        data={data.teacher}
        setData={(teacherData) => setData('teacher', teacherData)}
      />
      <JudgeModelSection data={data.judge} setData={(judgeData) => setData('judge', judgeData)} />
      <FormSection>
        <FineTunePageFooter
          canSubmit={canSubmit}
          onSuccess={onSuccess}
          data={data}
          ilabPipeline={ilabPipeline}
          ilabPipelineVersion={ilabPipelineVersion}
        />
      </FormSection>
    </Form>
  );
};

export default FineTunePage;
