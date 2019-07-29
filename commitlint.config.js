'use strict';

module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'scope-enum': [
      2,
      'always',
      [
        // pkg
        'starling',
        // sevices
        'librarian',
        // orher
        'go',
        'javascript',
        'typescript',
        'travis',
        'readme'
      ]
    ],
    'scope-empty': [2, 'never']
  }
};
