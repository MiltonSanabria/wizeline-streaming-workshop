import * as base from './base';

export const Dev: base.Config = {
    service: {
        vpcId: '<yourvpc>',
        env: {
            account: '<youraccount>',
            region: '<yourvpc>'
        }

    },

    tags: {
      ...base.tags,
      Environment: 'DEV'
    }
};