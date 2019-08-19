// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Page} from 'src/pageLayout'
import Collectors from 'src/telegrafs/components/Collectors'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Types
import {AppState, Organization} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class TelegrafsPage extends PureComponent<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="telegrafs" orgID={org.id}>
            <GetResources resource={ResourceTypes.Buckets}>
              <GetResources resource={ResourceTypes.Telegrafs}>
                <Collectors />
              </GetResources>
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = ({orgs: {org}}: AppState): StateProps => ({
  org,
})

export default connect<StateProps>(mstp)(TelegrafsPage)