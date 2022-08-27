import { TagsProps } from '../Interfaces/Itags';
import { WizelineStreamingProps } from '../wizeline-streaming-workshop-stack';

import { CDK_APP_NAME } from '../utils/constants';

/**
 * Base config interface.
 */
export interface Config {
  service: WizelineStreamingProps;
  tags: TagsProps;
}

export const tags: TagsProps = {
  Name: CDK_APP_NAME,
  Environment: 'UNKNOWN',
  Project: `Streaming_workshop`,
  Wizeline_team_contact: 'daniel.melguizo@wizeline.com'
};
