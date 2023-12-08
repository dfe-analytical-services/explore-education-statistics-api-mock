import slugify from 'slugify';
import { PublicationSummaryViewModel } from '../schema';

export const spcPublication = createPublication({
  id: '638482b6-d015-4798-a1a4-e2311253b3e1',
  title: 'Schools, pupils and their characteristics',
  summary:
    'School and pupil statistics for England including age, gender, free school meals (FSM), ethnicity, English as additional language (EAL), class size.',
  lastPublished: '2023-01-01T12:00:00',
});

export const pupilAbsencePublication = createPublication({
  id: '9676af6b-d563-41f4-d071-08da8f468680',
  title: 'Pupil attendance in schools',
  summary:
    'Pupil absence, including overall, authorised and unauthorised absence and persistent absence by reason and pupil characteristics for the full academic year.',
  lastPublished: '2023-01-01T12:00:00',
});

export const leoPublication = createPublication({
  id: 'b329f8e4-4191-4f0c-66a8-08d9daa3b093',
  title: 'LEO Graduate and Postgraduate Outcomes',
  summary:
    'Earnings and employment for higher education university graduates & postgraduates by subject studied and characteristics. Longitudinal Education Outcomes (LEO)..',
  lastPublished: '2023-01-01T12:00:00',
});

export const apprenticeshipsPublication = createPublication({
  id: 'cf0ec981-3583-42a5-b21b-3f2f32008f1b',
  title: 'Apprenticeships and traineeships',
  summary:
    'Apprenticeship and traineeship starts, achievements and participation. Includes breakdowns by age, sex, ethnicity, subject, provider, geography etc.',
  lastPublished: '2023-01-01T12:00:00',
});

// Don't put this in allPublications to keep it hidden
export const benchmarkPublication = createPublication({
  id: '1681557f-510f-446e-bc9a-f2c7a59d1cfa',
  title: 'Benchmarking',
  summary: 'N/A',
  lastPublished: '2023-01-01T12:00:00',
});

export const allPublications: PublicationSummaryViewModel[] = [
  spcPublication,
  pupilAbsencePublication,
  leoPublication,
  apprenticeshipsPublication,
];

function createPublication(
  data: Omit<PublicationSummaryViewModel, 'slug' | '_links'>,
): PublicationSummaryViewModel {
  return {
    ...data,
    slug: slugify(data.title.toLowerCase()),
    _links: {
      self: {
        href: `/api/v1/publications/${data.id}`,
      },
      dataSets: {
        href: `/api/v1/publications/${data.id}/data-sets`,
      },
    },
  };
}
